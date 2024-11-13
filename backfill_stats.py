import store_stats
from PGSQL import NFLDB_SQL

db = NFLDB_SQL()

if __name__ == '__main__':
	sql = "select distinct year, week from pfr_gameinfo where year >= 1994 order by year asc, week asc"
	results = db.execute_sql(sql)

	invalid_games = ['199711300phi.htm']

	for r in results:
		games = store_stats.get_gameid_teams(r['year'],r['week'])
		for gameid,teams in games.items():
			if gameid not in invalid_games:
				print(f"{r['year']}-{r['week']}-{gameid}-({teams[0]}/{teams[1]})")
				for team in teams:
					store_stats.store_offensive_summary(gameid,team)
					store_stats.store_drive_summary(gameid,team)
				if r['week'] > 1:
					store_stats.store_aggregate_performance_metrics(r['year'],r['week'],gameid,teams[0],teams[1])
					store_stats.store_flex_performance_metrics(r['year'],r['week'],gameid,teams[0],teams[1])
					store_stats.store_aggregate_performance_metrics(r['year'],r['week'],gameid,teams[1],teams[0])
					store_stats.store_flex_performance_metrics(r['year'],r['week'],gameid,teams[1],teams[0])
