from PGSQL import NFLDB_SQL
from PFRScraper import PFRScraper
import sys
import os
import argparse

db = NFLDB_SQL()
scraper = PFRScraper()

def process_commandline():
	# Create the parser
	parser = argparse.ArgumentParser(description="Process some arguments.")

	# Add arguments
	parser.add_argument("year", type=int,help="Year to scrape, required")
	parser.add_argument("--week", type=int,help="The week to scrape (optional)")

	# Parse the arguments
	args = parser.parse_args()

	# Access arguments
	return args

def main(year,week=None):
	sql = f"select link,year,week from pfr_weekly_links where year = {year} "
	if week != None:
		sql += f"and week = {week} "
	sql += "order by year,week asc"
	results = db.execute_sql(sql)
	data = []
	for r in results:
		url = r['link']
		print("Scraping {}-{}".format(r['year'],r['week']))
		gamelinks = scraper.get_game_links(url=url)
		for gl in gamelinks:
			data.append((os.path.basename(gl),r['year'],r['week']))
		if r['week'] != week:
			db.insert_data('pfr_gameinfo',data,pk='gameid',columns=['gameid','year','week'])
			data = []
			week = r['week']

if __name__ == '__main__':
	# args = process_commandline()
	# year = args.year
	# week = args.week
	# main(year,week)
	for i in range(1994,2025):
		main(i)
