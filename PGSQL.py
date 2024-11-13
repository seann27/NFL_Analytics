'''
Utility class for updating and extracting data from the postgres database
'''
import psycopg2, os
import psycopg2.extras
import traceback
from datetime import datetime

class NFLDB_SQL:
	conn = None

	def __init__(self,ssh_tunnel=None):
		self.username = "postgres"
		self.password = "password"
		self.database = "NFLDB"
		self.host = "localhost"
		self.port = 5435
		if ssh_tunnel:
			ssh_tunnel.start()
			self.port = ssh_tunnel.local_bind_port
			self.host = 'localhost'
		if NFLDB_SQL.conn is None or ssh_tunnel:
			try:
				print("establishing new sql connection...")
				NFLDB_SQL.conn = psycopg2.connect(
					database = self.database,
					user = self.username,
					password = self.password,
					host = self.host,
					port = self.port
				)
				print("Connected!")
			except Exception as err:
				print("Error connecting to database!\n{}".format(err))
				print(traceback.print_exc())
		self.conn = NFLDB_SQL.conn

		self.table_cols = {
				'pfr_gameinfo':[
					'gameid',
					'year',
					'week',
					'home_team',
					'home_wins',
					'home_losses',
					'away_team',
					'away_wins',
					'away_losses',
					'home_score',
					'away_score',
					"home_odds",
					"overunder",
					"timestamp",
					"home_ties",
					"away_ties"
				],
				'pfr_weekly_links':[
					'link',
					'year',
					'week',
					'is_postseason'
				],
				'pfr_teams': [
					'teamid',
					'abbrev',
					'name',
					'logo'
				],
				'pfr_total_offense':[
					'id',
					'gameid',
					'playerid',
					"team",
					"pass_cmp",
					"pass_att",
					"pass_yds",
					"pass_td",
					"pass_int",
					"pass_sacked",
					"pass_sacked_yds",
					"pass_long",
					"pass_rating",
					"rush_att",
					"rush_yds",
					"rush_td",
					"rush_long",
					"targets",
					"rec",
					"rec_yds",
					"rec_td",
					"rec_long",
					"fumbles",
					"fumbles_lost",
					"position",
					"depth"
				],
				'pfr_pbp':[
					'id',
					'quarter',
					'qtr_time_remain',
					'possession_change',
					'down',
					'yds_to_go',
					'location',
					'pbp_score_aw',
					'pbp_score_hm',
					'detail',
					'exp_pts_before',
					'exp_pts_after'
				],
				'offensive_summary': [
					'id',
					'gameid',
					'team',
					'opp',
					'pass_att',
					'pass_cmp',
					'pass_cmp_pct',
					'pass_yds',
					'pass_yds_per_att',
					'pass_yds_per_cmp',
					'pass_td',
					'pass_int',
					'pass_int_per_att',
					'pass_sacked',
					'pass_sacked_per_att',
					'pass_sacked_yds',
					'pass_long',
					'pass_rating',
					'rush_att',
					'rush_yds',
					'rush_yds_per_att',
					'rush_td',
					'rush_long',
					'fumbles',
					'fumbles_lost',
					'tds_per_100_yds',
					'pts_per_100_yds',
					'pct_attempts_rushing',
					'pct_attempts_passing',
					'pct_yards_rushing',
					'pct_yards_passing',
					'pct_tds_rushing',
					'pct_tds_passing',
					'rushing_gini',
					'passing_gini',
					'points_scored',
					'backup_qb1'
				],
				'game_summary_stats': [
					"id",
					"gameid",
					"team",
					"side",
					"avg_pass_att", "std_pass_att",
					"avg_pass_cmp", "std_pass_cmp",
					"avg_pass_cmp_pct", "std_pass_cmp_pct",
					"avg_pass_yds", "std_pass_yds",
					"avg_pass_yds_per_att", "std_pass_yds_per_att",
					"avg_pass_yds_per_cmp", "std_pass_yds_per_cmp",
					"avg_pass_td", "std_pass_td",
					"avg_pass_int", "std_pass_int",
					"avg_pass_int_per_att", "std_pass_int_per_att",
					"avg_pass_sacked", "std_pass_sacked",
					"avg_pass_sacked_per_att", "std_pass_sacked_per_att",
					"avg_pass_sacked_yds", "std_pass_sacked_yds",
					"avg_pass_long", "std_pass_long",
					"avg_pass_rating", "std_pass_rating",
					"avg_rush_att", "std_rush_att",
					"avg_rush_yds", "std_rush_yds",
					"avg_rush_yds_per_att", "std_rush_yds_per_att",
					"avg_rush_td", "std_rush_td",
					"avg_rush_long", "std_rush_long",
					"avg_fumbles", "std_fumbles",
					"avg_fumbles_lost", "std_fumbles_lost",
					"avg_tds_per_100_yds", "std_tds_per_100_yds",
					"avg_pts_per_100_yds", "std_pts_per_100_yds",
					"avg_pct_attempts_rushing", "std_pct_attempts_rushing",
					"avg_pct_attempts_passing", "std_pct_attempts_passing",
					"avg_pct_yards_rushing", "std_pct_yards_rushing",
					"avg_pct_yards_passing", "std_pct_yards_passing",
					"avg_pct_tds_rushing", "std_pct_tds_rushing",
					"avg_pct_tds_passing", "std_pct_tds_passing",
					"avg_rushing_gini", "std_rushing_gini",
					"avg_passing_gini", "std_passing_gini"
				],
				'play_by_play': [
					'id', # varchar 25, primary key
					'gameid', # varchar 25
					'quarter', # integer
					'time_remaining_half', # in minutes
					'possession_team', # varchar 5
					'possession_team_home', # true or false
					'drive_id', # varchar 25
					'down', # integer
					'yds_to_go', # integer
					'redzone', # true or false
					'location', # integer
					'location_detail', # varchar 10
					'detail', # text
					'home_score', # integer
					'away_score', # integer
					'exp_pts_before', # float
					'exp_pts_after', # float
					'rush', # true or false
					'rush_left', # true or false
					'rush_middle', # true or false
					'rush_right', # true or false
					'rush_guard', # true or false
					'rush_tackle', # true or false
					'rush_end', # true or false
					'rush_yds', # integer
					'pass', # true or false
					'pass_complete', # true or false
					'pass_short', # true or false
					'pass_deep', # true or false,
					'pass_left', # true or false
					'pass_middle', # true or false
					'pass_right', # true or false
					'pass_yds', # integer
					'sacked', # true or false
					'fumble', # true or false
					'turnover', # true or false
					'intercepted', # true or false
					'penalty', # true or false,
					'no_play', # true or false,
					'touchdown', # true or false
					'defensive_touchdown', # true or false
					"field_goal", # true or false
					"field_goal_yds", # integer
					"field_goal_good", # true or false
					"kickoff", # true or false
					"two_point_attempt", # true or false
					"extra_point", # true or false
					"extra_point_good", # true or false
					"punt", # true or false
					'quarterback', # varchar 50
					'quarterback_position', # varchar 5
					'quarterback_depth', # integer
					'running_back', # varchar 50
					'running_back_position', # varchar 5
					'running_back_depth', # integer
					'receiver', # varchar 50
					'receiver_position', # varchar 5
					'receiver_depth', # integer,
					'timeout', # boolean
				],
				'drive_summary': [
					'id', # varchar 50
					'gameid', # varchar 25
					'teamid',# varchar 25
					'team', # varchar 5
					'total_drives', # integer
					'plays_per_drive', # float
					'total_impact_plays', # integer
					'avg_ep_delta_rushing', # float
					'avg_ep_delta_passing', # float
					'third_down_conversion_pct', # float
					'tds_per_drive', # float
					'total_rz_drives', # integer
					'plays_per_rz_drive', # float
					'redzone_plays', # integer
					'tds_per_rzdrive', # float
					'redzone_rushing_plays', # integer
					'redzone_passing_plays', # integer
					'qb_rz_util', # float
					'rb_rz_util', # float
					'wr_rz_util', # float
					'te_rz_util' # float
				],
				'passing_performance': [
					'id', # varchar 50
					'gameid', # varchar 25
					'team', # varchar 5
					'opp', # varchar 5
					'l5_avg_pass_att',
					'l5_avg_pass_cmp',
					'l5_avg_pass_cmp_pct',
					'l5_avg_pass_yds',
					'l5_avg_pass_yds_per_att',
					'l5_avg_pass_yds_per_cmp',
					'l5_avg_pass_td',
					'l5_avg_pass_int',
					'l5_avg_pass_int_per_att',
					'l5_avg_pass_sacked',
					'l5_avg_pass_sacked_per_att',
					'l5_avg_pass_sacked_yds',
					'l5_avg_pass_long',
					'l5_avg_pass_rating',
					'l5_avg_pct_attempts_passing',
					'l5_avg_pct_yds_passing',
					'l5_avg_pct_tds_passing',
					'l5_avg_passing_gini',
					'l5_avg_avg_ep_delta_passing',
					'l5_avg_qb_rz_util',
					'l5_avg_wr_rz_util',
					'l5_avg_te_rz_util',
					'performance_pass_att',
					'performance_pass_cmp',
					'performance_pass_cmp_pct',
					'performance_pass_yds',
					'performance_pass_yds_per_att',
					'performance_pass_yds_per_cmp',
					'performance_pass_td',
					'performance_pass_int',
					'performance_pass_int_per_att',
					'performance_pass_sacked',
					'performance_pass_sacked_per_att',
					'performance_pass_sacked_yds',
					'performance_pass_long',
					'performance_pass_rating',
					'performance_pct_attempts_passing',
					'performance_pct_yds_passing',
					'performance_pct_tds_passing',
					'performance_passing_gini',
					'performance_avg_ep_delta_passing',
					'performance_qb_rz_util',
					'performance_wr_rz_util',
					'performance_te_rz_util'
				],
				'rushing_aggregate_performance': [
					'id',
					'gameid',
					'team',
					'opp',
					'l5_avg_rush_att',
					'l5_avg_rush_yds',
					'l5_avg_rush_yds_per_att',
					'l5_avg_rush_td',
					'l5_avg_rush_long',
					'l5_avg_pct_attempts_rushing',
					'l5_avg_pct_yards_rushing',
					'l5_avg_pct_tds_rushing',
					'l5_avg_rushing_gini',
					'l5_avg_avg_ep_delta_rushing',
					'l5_avg_avg_rb_rz_util',
					'performance_rush_att',
					'performance_rush_yds',
					'performance_rush_yds_per_att',
					'performance_rush_td',
					'performance_rush_long',
					'performance_pct_attempts_rushing',
					'performance_pct_yards_rushing',
					'performance_pct_tds_rushing',
					'performance_rushing_gini',
					'performance_avg_ep_delta_rushing',
					'performance_avg_rb_rz_util'
				],
				'rushing_performance':[
					'id', # varchar 50 primary key
					'gameid', # varchar 25
					'team', # varchar 5
					'opp', # varchar 5
					'position', # varchar 5
					'depth', # integer
					'rush_att', # integer
					'rush_yds', # integer
					'rush_yds_per_att',
					'rush_td', # integer
					'rush_long', # integer
					'rush_play_utilization',
					'total_play_utilization',
					'rush_yds_contribution_pct',
					'total_yds_contribution_pct',
					'total_td_contribution_pct',
					'l5_avg_rush_att',
					'l5_avg_rush_yds',
					'l5_avg_rush_yds_per_att',
					'l5_avg_rush_td',
					'l5_avg_rush_long',
					'l5_avg_rush_play_utilization',
					'l5_avg_total_play_utilization',
					'l5_avg_rush_yds_contribution_pct',
					'l5_avg_total_yds_contribution_pct',
					'l5_avg_total_td_contribution_pct',
					'performance_rush_att',
					'performance_rush_yds',
					'performance_rush_yds_per_att',
					'performance_rush_td',
					'performance_rush_long',
					'performance_rush_play_utilization',
					'performance_total_play_utilization',
					'performance_rush_yds_contribution_pct',
					'performance_total_yds_contribution_pct',
					'performance_total_td_contribution_pct',
				],
				'receiving_performance':[
					'id', # varchar 50 primary key
					'gameid', # varchar 25
					'team', # varchar 5
					'opp', # varchar 5
					'position', # varchar 5
					'depth', # integer
					'targets', # integer
					'rec', # integer
					'rec_per_target',
					'rec_yds', # integer
					'rec_yds_per_att',
					'rec_yds_per_rec',
					'rec_td', # integer
					'rec_long', # integer
					'rec_play_utilization',
					'total_play_utilization',
					'rec_yds_contribution_pct',
					'total_yds_contribution_pct',
					'total_td_contribution_pct',
					'l5_avg_targets',
					'l5_avg_rec',
					'l5_avg_rec_per_target',
					'l5_avg_rec_yds',
					'l5_avg_rec_yds_per_att',
					'l5_avg_rec_yds_per_rec',
					'l5_avg_rec_td',
					'l5_avg_rec_long',
					'l5_avg_rec_play_utilization',
					'l5_avg_total_play_utilization',
					'l5_avg_rec_yds_contribution_pct',
					'l5_avg_total_yds_contribution_pct',
					'l5_avg_total_td_contribution_pct',
					'performance_targets',
					'performance_rec',
					'performance_rec_per_target',
					'performance_rec_yds',
					'performance_rec_yds_per_att',
					'performance_rec_yds_per_rec',
					'performance_rec_td',
					'performance_rec_long',
					'performance_rec_play_utilization',
					'performance_total_play_utilization',
					'performance_rec_yds_contribution_pct',
					'performance_total_yds_contribution_pct',
					'performance_total_td_contribution_pct',
				]
		}

	# utility method for handling direct sql executions
	def execute_sql(self,statement):
		# print(statement)
		try:
			cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
			cur.execute(statement)
			self.conn.commit()
			return cur
		except (Exception, psycopg2.DatabaseError) as error:
			print ("Oops! An exception has occured:", error)
			print ("Exception TYPE:", type(error))
			# with open(f'{datetime.now().timestamp()}_error_stmt.txt','w') as f:
			# f.write(statement)
			self.conn.rollback()


	'''
	This upserts (insert/updates) the database given a table name and list of tuples
	- Assumes primary key of table is "id" by default
	- Can specify columns, can also specify what columns to update or whether to ignore updating if primary key already exists
	'''
	# if ignore_update is true, on data duplicate keys it will not update
	# data is a list of tuples
	def insert_data(self,table,data,pk="id",columns=None,ignore_update=False,update_cols=None,debug_message="",verbose=True):
		if len(data) == 0:
			print("Error! No data to insert. 0 records inserted "+debug_message)
			return []
		if columns == None:
			columns = self.table_cols[table].copy()
		colstring = ','.join(columns)
		placeholders = ','.join(["%s" for i in range(len(columns))])
		args = ','.join(self.conn.cursor().mogrify(f"({placeholders})",i).decode('utf-8') for i in data)

		statement = f"INSERT INTO {table} ({colstring}) VALUES {args} "
		statement += f"ON CONFLICT ({pk}) DO "
		if ignore_update:
			statement += "DO NOTHING "
		else:
			statement += "UPDATE SET "
			updates = []
			if update_cols:
				columns = update_cols.copy()
			else:
				columns = columns.copy()
			columns.remove(pk)
			for col in columns:
				updates.append(f"{col} = excluded.{col}")
			statement += ','.join(updates)
		# if verbose:
		# print(statement)
		cur = self.execute_sql(statement)
		if cur and verbose:
			print(f"Updated {cur.rowcount} records in {table}: {debug_message}")
