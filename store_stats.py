# imports
from PGSQL import NFLDB_SQL
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
import numpy as np
from tqdm import tqdm
import re

db = NFLDB_SQL()

# util methods
def calculate_gini(df, column):
	try:
		# Filter out rows where the specified column is greater than zero
		df_filtered = df[df[column] > 0]

		# Sort the filtered DataFrame by the specified column in ascending order
		df_sorted = df_filtered.sort_values(by=column)

		# Compute the cumulative sum of the specified column
		df_sorted['cumulative_sum'] = df_sorted[column].cumsum()

		# Calculate the total sum of the specified column
		total_sum = df_sorted[column].sum()

		# Compute the relative cumulative share
		df_sorted['relative_cumulative_share'] = df_sorted['cumulative_sum'] / total_sum

		# Calculate the Gini coefficient
		n = len(df_sorted)
		gini = (n + 1 - 2 * df_sorted['relative_cumulative_share'].sum()) / n

		return gini
	except Exception as err:
		print(err)
		return 0

def divide(numerator,denominator):
	if denominator == 0:
		return float(0)
	else:
		return float(numerator / denominator)

def cast_record(record):
	clean_record = []
	for d in record:
		if re.search(r'numpy',str(type(d))):
			clean_record.append(d.item())
		else:
			clean_record.append(d)
	return tuple(clean_record)

def get_gameid_teams(year,week):
	games = {}
	sql = f"""
		select distinct tot.gameid as gameid, tot.team as team
		from pfr_total_offense tot, pfr_gameinfo gi
		where gi.gameid = tot.gameid
		and gi.year = {year}
		and gi.week = {week}
	"""
	results = list(db.execute_sql(sql))
	for r in results:
		if r['gameid'] not in games:
			games[r['gameid']] = []
		games[r['gameid']].append(r['team'])

	return games

def get_games(year,week):
	sql = f"""
		select gi.gameid, tm1.metric_abbrev as home_team,tm2.metric_abbrev as away_team
		from pfr_gameinfo gi, pfr_teams tm1, pfr_teams tm2
		where gi.home_team = tm1.teamid
		and gi.away_team = tm2.teamid
		and year = {year}
		and week = {week}
	"""
	results = list(db.execute_sql(sql))
	return results

# store offensive summary
def store_offensive_summary(gameid,team):
	sql = f"""
		select gi.year,gi.week, tot.*,
		CASE
			WHEN tm1.metric_abbrev = tot.team THEN tm2.metric_abbrev
			ELSE tm1.metric_abbrev
		END AS opp,
		CASE
			WHEN tm1.metric_abbrev = tot.team THEN gi.home_score
			ELSE gi.away_score
		END AS points_scored
		from pfr_total_offense tot, pfr_gameinfo gi, pfr_teams tm1, pfr_teams tm2
		where gi.gameid = tot.gameid
		and gi.gameid = '{gameid}'
		and tot.team = '{team}'
		and tm1.teamid = gi.home_team
		and tm2.teamid = gi.away_team
	"""

	df = pd.read_sql(sql,db.conn)

	table_id = f"{gameid}-{team}"
	opp = df.iloc[0]['opp']
	points_scored = df.iloc[0]['points_scored']
	total_tds = df['rush_td'].sum() + df['pass_td'].sum()
	if total_tds == 0:
		pct_tds_rushing = pct_tds_passing = 0
	else:
		pct_tds_rushing = df['rush_td'].sum() / total_tds
		pct_tds_passing = df['pass_td'].sum() / total_tds

	backup_qb1 = False

	year = df.iloc[0]['year']
	week = df.iloc[0]['week']

	if week > 1:
		sql = f"""
			select distinct p.playerid from pfr_total_offense tot, pfr_gameinfo gi, pfr_players p
			where tot.gameid = gi.gameid
			and tot.team = '{team}'
			and tot.position = 'QB'
			and tot.depth = 1
			and tot.playerid = p.playerid
			and gi.week between {week-1} and {week}
			and gi.year = 2024
		"""

		if len(list(db.execute_sql(sql))) == 2:
			backup_qb1 = True

	record = (
		table_id,
		gameid,
		team,
		opp,
		df['pass_att'].sum(),
		df['pass_cmp'].sum(),
		divide(df['pass_cmp'].sum(),df['pass_att'].sum()),
		df['pass_yds'].sum(),
		divide(df['pass_yds'].sum(),df['pass_att'].sum()),
		divide(df['pass_yds'].sum(),df['pass_cmp'].sum()),
		df['pass_td'].sum(),
		df['pass_int'].sum(),
		divide(df['pass_int'].sum(),df['pass_att'].sum()),
		df['pass_sacked'].sum(),
		divide(df['pass_sacked'].sum(),df['pass_att'].sum()),
		df['pass_sacked_yds'].sum(),
		df['pass_long'].max(),
		df['pass_rating'].max(),
		df['rush_att'].sum(),
		df['rush_yds'].sum(),
		divide(df['rush_yds'].sum(),df['rush_att'].sum()),
		df['rush_td'].sum(),
		df['rush_long'].max(),
		df['fumbles'].sum(),
		df['fumbles_lost'].sum(),
		divide(df['pass_td'].sum() + df['rush_td'].sum(),df['pass_yds'].sum() + df['rush_yds'].sum()) * 100,
		divide(points_scored,df['pass_yds'].sum() + df['rush_yds'].sum()) * 100,
		divide(df['rush_att'].sum(),df['rush_att'].sum() + df['pass_att'].sum()),
		divide(df['pass_att'].sum(),df['rush_att'].sum() + df['pass_att'].sum()),
		divide(df['rush_yds'].sum(),df['rush_yds'].sum() + df['pass_yds'].sum()),
		divide(df['pass_yds'].sum(),df['rush_yds'].sum() + df['pass_yds'].sum()),
		pct_tds_rushing,
		pct_tds_passing,
		calculate_gini(df,'rush_att'),
		calculate_gini(df,'rec'),
		points_scored,
		backup_qb1
	)
	data = [cast_record(record)]
	db.insert_data('offensive_summary',data)

# store drive summary
def store_drive_summary(gameid,team):
	sql = f"select playerid, position from pfr_total_offense where gameid = '{gameid}'"
	player_ref = {}
	for p in list(db.execute_sql(sql)):
		player_ref[p['playerid']] = p['position']

	sql = f"""
		select pbp.*, tm.teamid from play_by_play pbp, pfr_gameinfo gi, pfr_teams tm
		where pbp.gameid = gi.gameid
		and pbp.possession_team = tm.metric_abbrev
		and (tm.teamid = gi.home_team or tm.teamid = gi.away_team)
		and pbp.possession_team = '{team}'
		and pbp.gameid = '{gameid}'
		and pbp.no_play = False
		and (pbp.pass = True or pbp.rush = True or pbp.sacked = True)
		order by pbp.quarter asc, pbp.time_remaining_half desc
	"""
	plays = list(db.execute_sql(sql))

	drives = []
	redzone_drives = []
	redzone_plays = 0
	total_rushing_plays = 0
	total_passing_plays = 0
	redzone_rushing_plays = 0
	redzone_passing_plays = 0
	ep_rushing_delta = 0
	ep_passing_delta = 0
	third_downs = 0
	third_down_conversions = 0
	touchdowns = 0
	rz_util = {
		'QB':0,
		'RB':0,
		'WR':0,
		'TE':0
	}

	teamid = plays[0]['teamid']
	team = plays[0]['possession_team']

	for idx,p in enumerate(plays):
		if p['drive_id'] not in drives:
			drives.append(p['drive_id'])

		if p['redzone']:
			redzone_plays += 1
			if p['drive_id'] not in redzone_drives:
				redzone_drives.append(p['drive_id'])

		if p['touchdown']:
			touchdowns += 1

		if p['rush']:
			total_rushing_plays += 1
			ep_rushing_delta += p['exp_pts_after'] - p['exp_pts_before']
			if p['redzone']:
				redzone_rushing_plays += 1
				if p['running_back'] in player_ref.keys():
					rb = player_ref[p['running_back']]
					if rb in rz_util.keys():
						rz_util[rb] += 1

		elif p['pass'] or p['sacked']:
			total_passing_plays += 1
			ep_passing_delta += p['exp_pts_after'] - p['exp_pts_before']
			if p['redzone']:
				redzone_passing_plays += 1
				if p['quarterback'] in player_ref.keys():
					qb = player_ref[p['quarterback']]
					if qb in rz_util.keys():
						rz_util[qb] += 1
				if p['receiver'] in player_ref.keys():
					wr = player_ref[p['receiver']]
					if wr in rz_util.keys():
						rz_util[wr] += 1

		if p['down'] == 3 and idx < len(plays) - 1:
			third_downs += 1
			if p['touchdown'] or( plays[idx+1]['drive_id'] == p['drive_id'] and plays[idx+1]['down'] == 1):
				third_down_conversions += 1

	# drives in game
	total_drives = len(drives)
	# total impact play count (offensive flex plays, excludes no-plays, penalties, special teams)
	total_impact_plays = total_rushing_plays + total_passing_plays
	# plays per drive
	plays_per_drive = divide( total_impact_plays, len(drives))
	# avg epa-epb per rushing impact play
	avg_ep_delta_rushing = divide(ep_rushing_delta, total_rushing_plays)
	# avg epa-epb per passing impact play
	avg_ep_delta_passing = divide(ep_passing_delta, total_passing_plays)
	# 3rd conversion percentage for impact plays
	third_down_conversion_pct = divide(third_down_conversions, third_downs)
	# touchdowns per drive
	tds_per_drive = divide(touchdowns, len(drives))
	# drives in the redzone
	total_rz_drives = len(redzone_drives)
	# plays per redzone drives
	plays_per_rz_drive = divide(redzone_plays, total_rz_drives)
	# touchdowns per redzone drive
	tds_per_rzdrive = divide(touchdowns, len(redzone_drives))
	# passing metrics in redzone
	redzone_rushing_plays = redzone_rushing_plays
	# rushing metrics in redzone
	redzone_passing_plays = redzone_passing_plays
	# QB redzone flex utilization (# of rz plays involving QB)
	qb_rz_util = divide(rz_util['QB'], redzone_plays)
	# RB redzone flex utilization (# of rz plays involving RB)
	rb_rz_util = divide(rz_util['RB'], redzone_plays)
	# WR redzone flex utilization (# of rz plays involving WR)
	wr_rz_util = divide(rz_util['WR'], redzone_plays)
	# TE redzone flex utilization (# of rz plays involving TE)
	te_rz_util = divide(rz_util['TE'], redzone_plays)

	data = [(
		f"{gameid}-{teamid}",
		gameid,
		teamid,
		team,
		total_drives,
		plays_per_drive,
		total_impact_plays,
		avg_ep_delta_rushing,
		avg_ep_delta_passing,
		third_down_conversion_pct,
		tds_per_drive,
		total_rz_drives,
		plays_per_rz_drive,
		redzone_plays,
		tds_per_rzdrive,
		redzone_rushing_plays,
		redzone_passing_plays,
		qb_rz_util,
		rb_rz_util,
		wr_rz_util,
		te_rz_util
	)]

	db.insert_data('drive_summary',data)

# store position summary

# calculate team averages (offensive and drive)

# calculate position averages

# calculate position performance
def store_aggregate_performance_metrics(year,week,gameid,team1,team2):
	sql = f"""
		select os.*, ds.* from offensive_summary os, pfr_gameinfo gi, drive_summary ds
		where gi.gameid = os.gameid
		and os.gameid = ds.gameid
		and gi.year = {year}
		and gi.week between {week-5} and {week-1}
		and os.team = '{team1}'
		and os.team = ds.team
	"""

	l5_avg_df = pd.read_sql(sql,db.conn)

	sql = f"""
		select os.*, ds.* from offensive_summary os, pfr_gameinfo gi, drive_summary ds
		where gi.gameid = os.gameid
		and os.gameid = ds.gameid
		and gi.year = {year}
		and gi.week = {week}
		and os.team = '{team1}'
		and os.team = ds.team
	"""

	stats_df = pd.read_sql(sql,db.conn)

	# passing
	passing_record = (
		f"{gameid}-{team1}",
		gameid,
		team1,
		team2,
		l5_avg_df['pass_att'].mean(),
		l5_avg_df['pass_cmp'].mean(),
		l5_avg_df['pass_cmp_pct'].mean(),
		l5_avg_df['pass_yds'].mean(),
		l5_avg_df['pass_yds_per_att'].mean(),
		l5_avg_df['pass_yds_per_cmp'].mean(),
		l5_avg_df['pass_td'].mean(),
		l5_avg_df['pass_int'].mean(),
		l5_avg_df['pass_int_per_att'].mean(),
		l5_avg_df['pass_sacked'].mean(),
		l5_avg_df['pass_sacked_per_att'].mean(),
		l5_avg_df['pass_sacked_yds'].mean(),
		l5_avg_df['pass_long'].mean(),
		l5_avg_df['pass_rating'].mean(),
		l5_avg_df['pct_attempts_passing'].mean(),
		l5_avg_df['pct_yards_passing'].mean(),
		l5_avg_df['pct_tds_passing'].mean(),
		l5_avg_df['passing_gini'].mean(),
		l5_avg_df['avg_ep_delta_passing'].mean(),
		l5_avg_df['qb_rz_util'].mean(),
		l5_avg_df['wr_rz_util'].mean(),
		l5_avg_df['te_rz_util'].mean(),
		divide(stats_df.iloc[0]['pass_att'],l5_avg_df['pass_att'].mean()),
		divide(stats_df.iloc[0]['pass_cmp'],l5_avg_df['pass_cmp'].mean()),
		divide(stats_df.iloc[0]['pass_cmp_pct'],l5_avg_df['pass_cmp_pct'].mean()),
		divide(stats_df.iloc[0]['pass_yds'],l5_avg_df['pass_yds'].mean()),
		divide(stats_df.iloc[0]['pass_yds_per_att'],l5_avg_df['pass_yds_per_att'].mean()),
		divide(stats_df.iloc[0]['pass_yds_per_cmp'],l5_avg_df['pass_yds_per_cmp'].mean()),
		divide(stats_df.iloc[0]['pass_td'],l5_avg_df['pass_td'].mean()),
		divide(stats_df.iloc[0]['pass_int'],l5_avg_df['pass_int'].mean()),
		divide(stats_df.iloc[0]['pass_int_per_att'],l5_avg_df['pass_int_per_att'].mean()),
		divide(stats_df.iloc[0]['pass_sacked'],l5_avg_df['pass_sacked'].mean()),
		divide(stats_df.iloc[0]['pass_sacked_per_att'],l5_avg_df['pass_sacked_per_att'].mean()),
		divide(stats_df.iloc[0]['pass_sacked_yds'],l5_avg_df['pass_sacked_yds'].mean()),
		divide(stats_df.iloc[0]['pass_long'],l5_avg_df['pass_long'].mean()),
		divide(stats_df.iloc[0]['pass_rating'],l5_avg_df['pass_rating'].mean()),
		divide(stats_df.iloc[0]['pct_attempts_passing'],l5_avg_df['pct_attempts_passing'].mean()),
		divide(stats_df.iloc[0]['pct_yards_passing'],l5_avg_df['pct_yards_passing'].mean()),
		divide(stats_df.iloc[0]['pct_tds_passing'],l5_avg_df['pct_tds_passing'].mean()),
		divide(stats_df.iloc[0]['passing_gini'],l5_avg_df['passing_gini'].mean()),
		divide(stats_df.iloc[0]['avg_ep_delta_passing'],l5_avg_df['avg_ep_delta_passing'].mean()),
		divide(stats_df.iloc[0]['qb_rz_util'],l5_avg_df['qb_rz_util'].mean()),
		divide(stats_df.iloc[0]['wr_rz_util'],l5_avg_df['wr_rz_util'].mean()),
		divide(stats_df.iloc[0]['te_rz_util'],l5_avg_df['te_rz_util'].mean()),
	)
	passing_data = [cast_record(passing_record)]
	db.insert_data('passing_performance',passing_data)

	rushing_aggregate_record = (
		f"{gameid}-{team1}",
		gameid,
		team1,
		team2,
		l5_avg_df['rush_att'].mean(),
		l5_avg_df['rush_yds'].mean(),
		l5_avg_df['rush_yds_per_att'].mean(),
		l5_avg_df['rush_td'].mean(),
		l5_avg_df['rush_long'].mean(),
		l5_avg_df['pct_attempts_rushing'].mean(),
		l5_avg_df['pct_yards_rushing'].mean(),
		l5_avg_df['pct_tds_rushing'].mean(),
		l5_avg_df['rushing_gini'].mean(),
		l5_avg_df['avg_ep_delta_rushing'].mean(),
		l5_avg_df['rb_rz_util'].mean(),
		divide(stats_df.iloc[0]['rush_att'],l5_avg_df['rush_att'].mean()),
		divide(stats_df.iloc[0]['rush_yds'],l5_avg_df['rush_yds'].mean()),
		divide(stats_df.iloc[0]['rush_yds_per_att'],l5_avg_df['rush_yds_per_att'].mean()),
		divide(stats_df.iloc[0]['rush_td'],l5_avg_df['rush_td'].mean()),
		divide(stats_df.iloc[0]['rush_long'],l5_avg_df['rush_long'].mean()),
		divide(stats_df.iloc[0]['pct_attempts_rushing'],l5_avg_df['pct_attempts_rushing'].mean()),
		divide(stats_df.iloc[0]['pct_yards_rushing'],l5_avg_df['pct_yards_rushing'].mean()),
		divide(stats_df.iloc[0]['pct_tds_rushing'],l5_avg_df['pct_tds_rushing'].mean()),
		divide(stats_df.iloc[0]['rushing_gini'],l5_avg_df['rushing_gini'].mean()),
		divide(stats_df.iloc[0]['avg_ep_delta_rushing'],l5_avg_df['avg_ep_delta_rushing'].mean()),
		divide(stats_df.iloc[0]['rb_rz_util'],l5_avg_df['rb_rz_util'].mean()),
	)
	rushing_aggregate_data = [cast_record(rushing_aggregate_record)]

	db.insert_data('rushing_aggregate_performance',rushing_aggregate_data)

def store_flex_performance_metrics(year,week,gameid,team1,team2):
	sql = f"""
		select * from pfr_total_offense where gameid = '{gameid}'
	"""
	total_offense_df = pd.read_sql(sql,db.conn)

	sql = f"""
		select tot.* from pfr_total_offense tot, pfr_gameinfo gi
		where tot.gameid = gi.gameid
		and gi.year = {year}
		and gi.week between {week-5} and {week-1}
		and tot.team = '{team1}'
	"""

	l5_avg_df = pd.read_sql(sql,db.conn)

	l5_avg_df['total_rush_att'] = l5_avg_df.groupby('gameid')['rush_att'].transform('sum')
	l5_avg_df['total_pass_att'] = l5_avg_df.groupby('gameid')['pass_att'].transform('sum')
	l5_avg_df['total_plays'] = l5_avg_df.groupby('gameid')[['rush_att','pass_att']].transform('sum').sum(axis=1)
	l5_avg_df['total_rush_yds'] = l5_avg_df.groupby('gameid')['rush_yds'].transform('sum')
	l5_avg_df['total_pass_yds'] = l5_avg_df.groupby('gameid')['pass_yds'].transform('sum')
	l5_avg_df['total_yds'] = l5_avg_df.groupby('gameid')[['rush_yds','pass_yds']].transform('sum').sum(axis=1)
	l5_avg_df['total_td'] = l5_avg_df.groupby('gameid')[['rush_td','pass_td']].transform('sum').sum(axis=1)
	l5_avg_df['rush_yds_per_att'] = l5_avg_df['rush_yds'] / l5_avg_df['rush_att']
	l5_avg_df['pass_yds_per_att'] = l5_avg_df['pass_yds'] / l5_avg_df['pass_att']
	l5_avg_df['rec_per_target'] = l5_avg_df['rec'] / l5_avg_df['targets']
	l5_avg_df['rec_yds_per_target'] = l5_avg_df['rec_yds'] / l5_avg_df['targets']
	l5_avg_df['rec_yds_per_rec'] = l5_avg_df['rec_yds'] / l5_avg_df['rec']

	positions = {
		'QB':[1],
		'RB':[1,2],
		'WR':[1,2,3],
		'TE':[1]
	}

	offense_df = total_offense_df[total_offense_df['team'] == team1].copy()
	rushing_performance_data = []
	receiving_performance_data = []
	for p,d in positions.items():
		pos = p
		for depth in d:
			l5_stats = l5_avg_df[(l5_avg_df['position'] == pos) & (l5_avg_df['depth'] == depth)]

			stats = offense_df[(offense_df['position'] == pos) & (offense_df['depth'] == depth) & (offense_df['rush_att'] > 0)].copy()
			if len(stats) == 1 and len(l5_stats) > 0:
				stat = stats.iloc[0]

				rush_play_util = divide(stat['rush_att'], offense_df['rush_att'].sum())
				total_play_util = divide(stat['rush_att'], offense_df['rush_att'].sum() + offense_df['pass_att'].sum())
				rush_yds_util = divide(stat['rush_yds'], offense_df['rush_yds'].sum())
				total_yds_util = divide(stat['rush_yds'], offense_df['rush_yds'].sum() + offense_df['pass_yds'].sum())
				total_td_contrib = divide(stat['rush_td'], offense_df['rush_td'].sum() + offense_df['pass_td'].sum())

				l5_rush_att_avg = (l5_stats['rush_att']/l5_stats['total_rush_att']).mean()
				l5_rush_tot_avg = (l5_stats['rush_att']/l5_stats['total_plays']).mean()
				l5_rush_yds_avg = (l5_stats['rush_yds']/l5_stats['total_rush_yds']).mean()
				l5_rush_yds_tot_avg = (l5_stats['rush_yds']/l5_stats['total_yds']).mean()
				l5_rush_td_contrib = (l5_stats['rush_td']/l5_stats['total_td']).mean()

				rush_record = (
					stat['id'], # id
					stat['gameid'], # gameid
					team1, # team
					team2, # opp
					pos, # position
					depth, # depth
					stat['rush_att'], # rush attempts
					stat['rush_yds'], # rush yards
					divide(stat['rush_yds'],stat['rush_att']), # rush yards per attempt
					stat['rush_td'], # rushing tds
					stat['rush_long'], # longest rush
					rush_play_util, # rushing utilization (number of rushes by player / team)
					total_play_util, # number of rushes by player / total team plays
					rush_yds_util, # rush yds utilization (number of rush yds by player / team rush yds)
					total_yds_util, # total rush yds utilization (rush yds by player / total team yds)
					total_td_contrib, # total rush td / total td
					l5_stats['rush_att'].mean(),
					l5_stats['rush_yds'].mean(),
					l5_stats['rush_yds_per_att'].mean(),
					l5_stats['rush_td'].mean(),
					l5_stats['rush_long'].mean(),
					l5_rush_att_avg,
					l5_rush_tot_avg,
					l5_rush_yds_avg,
					l5_rush_yds_tot_avg,
					l5_rush_td_contrib,
					divide(stat['rush_att'],l5_stats['rush_att'].mean()),
					divide(stat['rush_yds'],l5_stats['rush_yds'].mean()),
					divide(divide(stat['rush_yds'],stat['rush_att']),l5_stats['rush_yds_per_att'].mean()),
					divide(stat['rush_td'],l5_stats['rush_td'].mean()),
					divide(stat['rush_long'],l5_stats['rush_long'].mean()),
					divide(rush_play_util,l5_rush_att_avg),
					divide(total_play_util,l5_rush_tot_avg),
					divide(rush_yds_util,l5_rush_yds_avg),
					divide(total_yds_util,l5_rush_yds_tot_avg),
					divide(total_td_contrib,l5_rush_td_contrib)
				)
				rushing_performance_data.append(cast_record(rush_record))

			stats = offense_df[(offense_df['position'] == pos) & (offense_df['depth'] == depth) & (offense_df['targets'] > 0)].copy()
			if len(stats) == 1 and len(l5_stats) > 0:
				stat = stats.iloc[0]

				rec_play_util = divide(stat['targets'], offense_df['pass_att'].sum())
				total_play_util = divide(stat['targets'], offense_df['rush_att'].sum() + offense_df['pass_att'].sum())
				rec_yds_util = divide(stat['rec_yds'], offense_df['rec_yds'].sum())
				total_yds_util = divide(stat['rec_yds'], offense_df['rush_yds'].sum() + offense_df['pass_yds'].sum())
				total_td_contrib = divide(stat['rec_td'], offense_df['rush_td'].sum() + offense_df['pass_td'].sum())

				l5_target_share_avg = (l5_stats['targets']/l5_stats['total_pass_att']).mean()
				l5_target_tot_avg = (l5_stats['targets']/l5_stats['total_plays']).mean()
				l5_rec_yds_avg = (l5_stats['rec_yds']/l5_stats['total_pass_yds']).mean()
				l5_rec_yds_tot_avg = (l5_stats['rec_yds']/l5_stats['total_yds']).mean()
				l5_rec_td_contrib = (l5_stats['rec_td']/l5_stats['total_td']).mean()

				receiving_record = (
					stat['id'], # id
					stat['gameid'], # gameid
					team1, # team
					team2, # opp
					pos, # position
					depth, # depth
					stat['targets'],
					stat['rec'],
					divide(stat['rec'],stat['targets']),
					stat['rec_yds'],
					divide(stat['rec_yds'],stat['targets']),
					divide(stat['rec_yds'],stat['rec']),
					stat['rec_td'],
					stat['rec_long'],
					rec_play_util,
					total_play_util,
					rec_yds_util,
					total_yds_util,
					total_td_contrib,
					l5_stats['targets'].mean(),
					l5_stats['rec'].mean(),
					l5_stats['rec_per_target'].mean(),
					l5_stats['rec_yds'].mean(),
					l5_stats['rec_yds_per_target'].mean(),
					l5_stats['rec_yds_per_rec'].mean(),
					l5_stats['rec_td'].mean(),
					l5_stats['rec_long'].mean(),
					l5_target_share_avg,
					l5_target_tot_avg,
					l5_rec_yds_avg,
					l5_rec_yds_tot_avg,
					l5_rec_td_contrib,
					divide(stat['targets'],l5_stats['targets'].mean()),
					divide(stat['rec'],l5_stats['rec'].mean()),
					divide(divide(stat['rec'],stat['targets']),l5_stats['rec_per_target'].mean()),
					divide(stat['rec_yds'],l5_stats['rec_yds'].mean()),
					divide(divide(stat['rec_yds'],stat['targets']),l5_stats['rec_yds_per_target'].mean()),
					divide(divide(stat['rec_yds'],stat['rec']),l5_stats['rec_yds_per_rec'].mean()),
					divide(stat['rec_td'],l5_stats['rec_td'].mean()),
					divide(stat['rec_long'],l5_stats['rec_long'].mean()),
					divide(rec_play_util,l5_target_share_avg),
					divide(total_play_util,l5_target_tot_avg),
					divide(rec_yds_util,l5_rec_yds_avg),
					divide(total_yds_util,l5_rec_yds_tot_avg),
					divide(total_td_contrib,l5_rec_td_contrib)
				)

				receiving_performance_data.append(cast_record(receiving_record))
	db.insert_data('rushing_performance',rushing_performance_data,pk="id")
	db.insert_data('receiving_performance',receiving_performance_data,pk="id")
