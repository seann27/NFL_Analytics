import download_weekly_pages
from scrape_game import process_games
import upload_weekly_pbp
import postprocess_players
import sys
import store_stats

if __name__ == '__main__':
	year = int(sys.argv[1])
	week = int(sys.argv[2])

	print(f"Downloading pages for {year}, week {week}")
	download_weekly_pages.main(year,week)

	print(f"Processing games for {year}, week {week}")
	process_games(year,week)

	print("Postprocessing players")
	postprocess_players.main()
	postprocess_players.add_positions()
	postprocess_players.add_position_depths()

	print(f"Processing play by play data for {year}, week {week}")
	upload_weekly_pbp.main(year,week)

	print("Calculating advanced statistics...")
	games = store_stats.get_gameid_teams(year,week)
	for gameid,teams in games.items():
		for team in teams:
			print(f"{year}-{week}-{gameid}-{team}")
			print(f"Calculating and uploading game stats...")
			# crunch game stats from pfr_total_offense
			store_stats.store_offensive_summary(gameid,team)
			print(f"Parsing and uploading drive stats...")
			# crunch drive stats from play_by_play
			store_stats.store_drive_summary(gameid,team)

		print("Calculating position performance")
		# Calculate position performance, implied projections (avg attempts per game * avg yds per attempt), redzone utilization, etc...), actual/implied
		if week > 1:
			store_stats.store_aggregate_performance_metrics(year,week,gameid,teams[0],teams[1])
			store_stats.store_flex_performance_metrics(year,week,gameid,teams[0],teams[1])
			store_stats.store_aggregate_performance_metrics(year,week,gameid,teams[1],teams[0])
			store_stats.store_flex_performance_metrics(year,week,gameid,teams[1],teams[0])

	# print("Calculating averages")
	# Combine drive stats and game stats, calculate averages

	# print("Create offensive/defensive play clustering profiles")
