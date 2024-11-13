import upload_pbp
import numpy as np
import pandas as pd
import sys
import re
from PGSQL import NFLDB_SQL
pd.set_option("future.no_silent_downcasting", True)
from tqdm import tqdm

db = NFLDB_SQL()

schema = {
    'id': 'str',  # for varchar(25)
	'gameid': 'str',
    'quarter': 'int64',
    'time_remaining_half': 'int64',
    'possession_team': 'str',  # for varchar(5)
    'possession_team_home': 'bool',
    'drive_id': 'str',  # for varchar(25)
    'down': 'int64',
    'yds_to_go': 'int64',
    'redzone': 'bool',
    'location': 'int64',
    'location_detail': 'str',  # for varchar(10)
    'detail': 'str',  # for text
    'home_score': 'int64',
    'away_score': 'int64',
    'exp_pts_before': 'float64',
    'exp_pts_after': 'float64',
    'rush': 'bool',
    'rush_left': 'bool',
    'rush_middle': 'bool',
    'rush_right': 'bool',
    'rush_tackle': 'bool',
    'rush_end': 'bool',
    'rush_yds': 'int64',
    'pass': 'bool',
    'pass_complete': 'bool',
    'pass_short': 'bool',
    'pass_deep': 'bool',
    'pass_left': 'bool',
    'pass_middle': 'bool',
    'pass_right': 'bool',
    'pass_yds': 'int64',
    'sacked': 'bool',
    'fumble': 'bool',
    'turnover': 'bool',
    'intercepted': 'bool',
    'penalty': 'bool',
    'no_play': 'bool',
    'touchdown': 'bool',
    'defensive_touchdown': 'bool',
    'field_goal': 'bool',
    'field_goal_yds': 'int64',
    'field_goal_good': 'bool',
    'kickoff': 'bool',
    'two_point_attempt': 'bool',
    'extra_point': 'bool',
    'extra_point_good': 'bool',
    'punt': 'bool',
    'quarterback': 'str',  # for varchar(50)
    'quarterback_position': 'str',  # for varchar(5)
    'quarterback_depth': 'float64',
    'running_back': 'str',  # for varchar(50)
    'running_back_position': 'str',  # for varchar(5)
    'running_back_depth': 'float64',
    'receiver': 'str',  # for varchar(50)
    'receiver_position': 'str',  # for varchar(5)
    'receiver_depth': 'float64',
    'timeout':'bool'
}

def main(year,week):
	sql = f"select distinct gi.gameid, gi.year, gi.week from pfr_total_offense tot, pfr_gameinfo gi where gi.gameid=tot.gameid and gi.year = {year} and gi.week = {week}"
	results = list(db.execute_sql(sql))
	for r in results:
		try:
			print(f"{r['year']}-{r['week']}")
			df = upload_pbp.main(r['gameid'])
			df = df.astype(schema)
			data = [tuple(row) for row in df.values]
			db.insert_data('play_by_play',data,columns=df.columns.to_list())
		except Exception as err:
			print("Error processing {}".format(r['gameid']))
			with open("problem_games.txt","a") as wh:
				wh.write(r['gameid'])
				wh.write("\n")


if __name__ == '__main__':
	main(sys.argv[1],sys.argv[2])
