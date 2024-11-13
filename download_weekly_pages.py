from download_gamepage import download_soup
from PGSQL import NFLDB_SQL
import argparse

db = NFLDB_SQL()

def process_commandline():
	# Create the parser
	parser = argparse.ArgumentParser(description="Process some arguments.")

	# Add arguments
	parser.add_argument("year", type=int,help="Year to scrape, required")
	parser.add_argument("week", type=int,help="The week to scrape (optional)")

	# Parse the arguments
	args = parser.parse_args()

	# Access arguments
	return args

def main(year,week):
	sql = f"""
		SELECT gameid FROM pfr_gameinfo
		WHERE year = {year}
		AND week = {week}
	"""
	results = db.execute_sql(sql)
	for r in results:
		download_soup(r['gameid'])

if __name__ == '__main__':
	args = process_commandline()
	main(args.year,args.week)
