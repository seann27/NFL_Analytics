import os
import sys
from PGSQL import NFLDB_SQL
import polars as pl
import pandas as pd
from glob import glob
import re
from tqdm import tqdm
import numpy as np

db = NFLDB_SQL()

def add_metrics(info,play_data):
	for k,v in info.items():
		play_data[k] = v
	return play_data

def forward_fill_series(series,default_value):
	series = series.ffill()
	series = series.fillna(default_value)
	return series

def time_to_seconds(time_str):
	comps = time_str.split(':')
	if len(comps) > 1:
		try:
			minutes, seconds = map(int, time_str.split(':'))
			return minutes * 60 + seconds
		except Exception as err:
			print(err, time_str, comps, len(comps))
	else:
		return np.nan

def process_play(play):
	challenged = re.compile('.*overturned\. (.*)')
	challenged_match = challenged.match(play)
	if challenged_match:
		play = challenged_match.group(1)
	play_data = {}
	info,match = parse_rush(play)
	play_data = add_metrics(info,play_data)
	info,match = parse_pass(play)
	play_data = add_metrics(info,play_data)
	info,match = parse_dst_and_scoring(play)
	play_data = add_metrics(info,play_data)
	info,match = parse_misc(play)
	play_data = add_metrics(info,play_data)
	return play_data

def parse_rush(play):
	info = {
		'rush':False,
		'rush_left':False,
		'rush_middle':False,
		'rush_right':False,
		'rush_guard':False,
		'rush_tackle':False,
		'rush_end':False,
		'running_back':None,
		'rush_yds':0
	}

#     rush_regex = re.compile(r'[A-Za-z\s]+:\[(?P<flex_player>.+?)\](?:\s+(?P<direction>(left|right|middle)))?(?:\s+(?P<gap>(tackle|end)))?(?:\s+kneels)? for (?P<rush_yds>-?\d+ yards)?')
	rush_regex = re.compile(r'''
		[A-Za-z\s\-.']+:\[(?P<flex_player>/players/[A-Za-z0-9/]+\.htm)\]
		(?:\s+scrambles)?
		(?:\s+(up\sthe\s)?(?P<direction>left|right|middle))?
		(?:\s+(?P<gap>guard|tackle|end))?
		(?:\s+kneels)?
		\sfor\s
		((?P<rush_yds>-?\d+)\s(yard))?
		(?P<no_gain>no\sgain)?
	''', re.VERBOSE)

	match = rush_regex.match(play)
	match_success = False
	if match:
		match_success = True
		info['rush'] = True
		if match.group('direction'):
			info[f"rush_{match.group('direction')}"] = True
		if match.group('gap'):
			info[f"rush_{match.group('gap')}"] = True
		info['running_back'] = match.group('flex_player')
		if match.group('rush_yds'):
			info['rush_yds'] = match.group('rush_yds')
		if match.group('no_gain'):
			info['rush_yds'] = 0
	return info, match_success

def parse_pass(play):
	info = {
		'pass':False,
		'pass_complete':False,
		'pass_short':False,
		'pass_deep':False,
		'pass_left':False,
		'pass_middle':False,
		'pass_right':False,
		'pass_yds':0,
		'intercepted':False,
		'quarterback':None,
		'receiver':None
	}

	pass_regex = re.compile(r'''
		[A-Za-z\s\-.']+:\[(?P<qb_player>/players/[A-Za-z0-9/]+\.htm)\]\s+
		pass
		(?:\s+(?P<pass_complete>complete|incomplete))?
		(?:\s+(?P<pass_type>short|deep))?
		(?:\s+(?P<direction>left|right|middle))?
		(?:\s+to)?
		(?:\s+intended\sfor)?
		(?:\s+[A-Za-z\s\-.']+:\[(?P<flex_player>/players/[A-Za-z0-9/]+\.htm)\])?
		(?:\s+for\s+(?P<pass_yds>-?\d+)\s+yard?)?
		(?:\s+is\s(?P<intercepted>intercepted))?
	''', re.VERBOSE)


	match = pass_regex.match(play)
	match_success = False
	if match:
		match_success = True
#         print(match)
		info['pass'] = True
		if match.group('pass_complete') == 'complete':
			info['pass_complete'] = True
		if match.group('pass_type'):
			info[f"pass_{match.group('pass_type')}"] = True
		if match.group('direction'):
			info[f"pass_{match.group('direction')}"] = True
		info['quarterback'] = match.group('qb_player')
		if match.group('flex_player'):
			info['receiver'] = match.group('flex_player')
		if match.group('pass_yds'):
			info['pass_yds'] = match.group('pass_yds')
		if match.group('intercepted'):
			info['intercepted'] = True
	return info, match_success

def parse_dst_and_scoring(play):
	info = {
		"touchdown":False,
		"field_goal":False,
		"field_goal_yds":0,
		"field_goal_good":False,
		"kickoff":False,
		"two_point_attempt":False,
		"extra_point":False,
		"extra_point_good":False,
		"punt":False,
		"sacked":False,
		"fumble":False
	}

	scoring_dst_regex = re.compile(r'''
		(?:(?P<two_point_attempt>Two\sPoint\sAttempt\:\s))?
		[A-Za-z\s\-.']+:\[(?P<player>/players/[A-Za-z0-9/]+\.htm)\]\s+
		(?:.*?(?P<sacked>sacked))?
		(?:.*?(?P<fumble>fumbles))?
		(?:.*?(?P<punt>punts))?
		(?:.*?(?P<touchdown>touchdown))?
		(?:\s+(?P<kickoff>kicks\s(off|onside)))?
		(?:\s+(?P<extra_point>kicks\sextra\spoint))?
		(?:\s+(?P<extra_point_good>good))?
		(?:\s+(?P<field_goal_yds>\d+)\syard\s(?P<field_goal>field\sgoal))?
		(?:\s+(?P<field_goal_good>good))?
	''',re.VERBOSE)

	match = scoring_dst_regex.match(play)

	match_success = False
	if match:
		match_success = True

		if match.group('two_point_attempt'):
			info['two_point_attempt'] = True
		if match.group('touchdown'):
			info['touchdown'] = True
		if match.group('sacked'):
			info['pass'] = True
			info['quarterback'] = match.group('player')
			info['sacked'] = True
		if match.group('fumble'):
			info['fumble'] = True
		if match.group('punt'):
			info['punt'] = True
		if match.group('kickoff'):
			info['kickoff'] = True
		if match.group('extra_point'):
			info['extra_point'] = True
			if match.group('extra_point_good'):
				info['extra_point_good'] = True
		if match.group('field_goal'):
			info['field_goal'] = True
			if match.group('field_goal_yds'):
				info['field_goal_yds'] = match.group('field_goal_yds')
			if match.group('field_goal_good'):
				info['field_goal_good'] = True

	return info, match_success

def parse_misc(play):
	play = play.lower()
	info = {
		'timeout':False,
		'penalty':False,
		'no_play':False
	}
	misc_regex = re.compile(r'''
	(?:(?P<timeout>timeout))?
	(?:.*(?P<penalty>penalty))?
	(?:.*(?P<no_play>no\splay))?
	''',re.VERBOSE)

	match = misc_regex.match(play)
	match_success = False
	if match:
		match_success = True

	if match.group('timeout'):
		info['timeout'] = True
	if match.group('penalty'):
		info['penalty'] = True
	if match.group('no_play'):
		info['no_play'] = True

	return info, match_success

def get_references(gameid):
	player_ref = {}
	sql = f"""
		select tot.playerid,tot.team,tot.position,tot.depth,tm.metric_abbrev as home_team from pfr_total_offense tot, pfr_gameinfo gi, pfr_teams tm
		where tot.gameid = gi.gameid
		and gi.home_team = tm.teamid
		and tot.gameid = '{gameid}'
	"""
	results = list(db.execute_sql(sql))
	team_abbrev = {}
	for r in results:
		player_ref[r['playerid']] = {
			'team': r['team'],
			'position':r['position'],
			'depth':r['depth']
		}
		if r['team'] == r['home_team']:
			player_ref[r['playerid']]['home'] = True
			team_abbrev[r['team']] = True
		else:
			player_ref[r['playerid']]['home'] = False
			team_abbrev[r['team']] = False
	return player_ref,team_abbrev

def process_plays(gameid):
	file = os.path.join('pbp_data',gameid.replace('.htm','') + "_pbp.parquet")
	if os.path.exists(file) == False:
		print(f"PBP file for {gameid} doesn't exist: {file}")
		exit()
	df = pl.read_parquet(file)
	player_ref, team_abbrev = get_references(gameid)

	plays = []
	drive = 1
	drive_teams = {
		'0':None,
		'1':None
	}
	idx = 1
	for d in df.iter_rows(named=True):
		if d['possession_change'] == True:
			drive += 1

		# initialize info
		info = {
			'id': f"{gameid}-{idx}",
			'gameid':gameid,
			'drive_id': f"{gameid}-{drive}",
			'quarter': d['quarter'],
			'time_remaining_half': time_to_seconds(d['qtr_time_remain']),
			'down': d['down'],
			'yds_to_go': d['yds_to_go'],
			'home_score': d['pbp_score_hm'],
			'away_score': d['pbp_score_aw'],
			'exp_pts_before': d['exp_pts_before'],
			'exp_pts_after': d['exp_pts_after']
		}

		# postprocess info metadata
		if re.search(r'OT',info['quarter']):
			info['quarter'] = 5
		elif re.search(r'\d+',info['quarter']):
			info['quarter'] = int(info['quarter'])
		if info['quarter'] == 1 or info['quarter'] == 3:
			if info['time_remaining_half'] != np.nan:
				info['time_remaining_half'] += time_to_seconds("15:00")

		play_data = process_play(d['detail'])
		info.update(play_data)

		possession_team = None
		if info['quarterback']:
			if info['quarterback'] not in player_ref.keys():
				info['quarterback_position'] = 'QB'
				info['quarterback_depth'] = 2
			else:
				possession_team = player_ref[info['quarterback']]['team']
				info['possession_team_home'] = player_ref[info['quarterback']]['home']
				info['quarterback_position'] = player_ref[info['quarterback']]['position']
				info['quarterback_depth'] = player_ref[info['quarterback']]['depth']
		if info['receiver']:
			if info['receiver'] not in player_ref.keys():
				info['receiver_position'] = 'WR'
				info['receiver_depth'] = 5
			else:
				info['receiver_position'] = player_ref[info['receiver']]['position']
				info['receiver_depth'] = player_ref[info['receiver']]['depth']
		if info['running_back']:
			if info['running_back'] not in player_ref.keys():
				info['running_back_position'] = 'RB'
				info['running_back_depth'] = 3
			else:
				possession_team = player_ref[info['running_back']]['team']
				info['possession_team_home'] = player_ref[info['running_back']]['home']
				info['running_back_position'] = player_ref[info['running_back']]['position']
				info['running_back_depth'] = player_ref[info['running_back']]['depth']

		if possession_team == None:
			possession_team = drive_teams[str(drive % 2)]
			if possession_team != None:
				info['possession_team_home'] = team_abbrev[possession_team]

		if possession_team:
			pattern = re.compile(r'(\w{3}) (\d+)')
			match = pattern.match(d['location'])
			if match:
				if possession_team == match.group(1):
					info['location'] = 100 - int(match.group(2))
				else:
					info['location'] = int(match.group(2))

			# get the drive
			drive_team = drive % 2
			if drive_team == 0:
				drive_teams['0'] = possession_team
				for k in team_abbrev.keys():
					if k != possession_team:
						drive_teams['1'] = k
			else:
				drive_teams['1'] = possession_team
				for k in team_abbrev.keys():
					if k != possession_team:
						drive_teams['0'] = k
		info['location_detail'] = d['location']
		info['possession_team'] = possession_team
		info['detail'] = d['detail']

		plays.append(info)
		idx += 1

	plays_df = pd.DataFrame(plays)

	# postprocess
	for idx,row in plays_df[plays_df['location'].isna()].iterrows():
		drive = int(row['drive_id'].split('-')[1])
		possession_team = drive_teams[str(drive % 2)]
		plays_df.loc[idx, 'possession_team'] = possession_team
		plays_df.loc[idx, 'possession_team_home'] = team_abbrev[possession_team]

		pattern = re.compile(r'(\w{3}) (\d+)')
		match = pattern.match(row['location_detail'])
		if match:
			if possession_team == match.group(1):
				plays_df.loc[idx,'location'] = 100 - int(match.group(2))
			else:
				plays_df.loc[idx,'location'] = int(match.group(2))

	plays_df['redzone'] = False
	for idx,row in plays_df[plays_df['location'] <= 25].iterrows():
		plays_df.loc[idx,'redzone'] = True

	plays_df['turnover'] = False
	for idx,row in plays_df[(plays_df['fumble'] == True) | (plays_df['intercepted'] == True)].iterrows():
		if idx < len(plays_df)-2:
			if plays_df.iloc[idx+1]['possession_team'] != row['possession_team']:
				plays_df.loc[idx,'turnover'] = True

	plays_df.replace('', np.nan, inplace=True)
	plays_df['defensive_touchdown'] = False
	for idx,row in plays_df[plays_df['exp_pts_after'].astype('float') == -7].iterrows():
		if row['touchdown'] == True:
			plays_df.loc[idx,'defensive_touchdown'] = True
			plays_df.loc[idx,'touchdown'] = False

	plays_df['quarter'] = forward_fill_series(plays_df['quarter'],1)
	plays_df['time_remaining_half'] = forward_fill_series(plays_df['time_remaining_half'],1800)
	plays_df['down'] = forward_fill_series(plays_df['down'],1)
	plays_df['yds_to_go'] = forward_fill_series(plays_df['yds_to_go'],10)
	plays_df['home_score'] = forward_fill_series(plays_df['home_score'],0)
	plays_df['away_score'] = forward_fill_series(plays_df['away_score'],0)
	plays_df['exp_pts_before'] = forward_fill_series(plays_df['exp_pts_before'],0)
	plays_df['exp_pts_after'] = forward_fill_series(plays_df['exp_pts_after'],0)
	plays_df['location'] = forward_fill_series(plays_df['location'],35)
	if re.search(r'coin toss',plays_df.iloc[0]['detail'].lower()):
		plays_df.loc[0,'no_play'] = True
	plays_df['quarterback_depth'] = plays_df['quarterback_depth'].fillna(0)
	plays_df['running_back_depth'] = plays_df['running_back_depth'].fillna(0)
	plays_df['receiver_depth'] = plays_df['receiver_depth'].fillna(0)

	return plays_df

def main(gameid):
	plays_df = process_plays(gameid)
	return plays_df

if __name__ == '__main__':
	main(sys.argv[1])
