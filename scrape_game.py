from PGSQL import NFLDB_SQL
from PFRScraper import PFRScraper
import sys,os
from datetime import datetime
import argparse

db = NFLDB_SQL()
db.table_cols['pfr_total_offense'].remove("position")
db.table_cols['pfr_total_offense'].remove("depth")
scraper = PFRScraper()

def construct_gameinfo_record(gameinfo,year,week):
	record = (gameinfo['gameid'],year,week)
	record += (gameinfo['home_team']['teamid'],gameinfo['home_team']['wins'],gameinfo['home_team']['losses'],)
	record += (gameinfo['away_team']['teamid'],gameinfo['away_team']['wins'],gameinfo['away_team']['losses'],)
	record += (gameinfo['home_team']['score'],gameinfo['away_team']['score'],)
	record += (gameinfo['home_odds'],gameinfo['vegas_total'],gameinfo['timestamp'])
	record += (gameinfo['home_team']['ties'],gameinfo['away_team']['ties'])
	return record

def construct_pfr_total_offense_record(datarow):
	dr = tuple()
	for c in db.table_cols['pfr_total_offense']:
		dr += (datarow[c],)
	return dr

def insert_teams(gameinfo):
	team_data = []
	team_data.append(((gameinfo['home_team']['teamid']),gameinfo['home_team']['abbrev'],gameinfo['home_team']['name'],gameinfo['home_team']['logo']))
	team_data.append(((gameinfo['away_team']['teamid']),gameinfo['away_team']['abbrev'],gameinfo['away_team']['name'],gameinfo['away_team']['logo']))
	db.insert_data("pfr_teams",team_data,pk="teamid")

def scrape_game_data(gameid,year,week):
	url = scraper.uri+f"/boxscores/{gameid}"
	scraper.load_soup(url)
	game_ts = scraper.get_game_timestamp()
	home_team,away_team,gameteams_soup = scraper.get_teams()
	data = [(gameid,year,week,home_team['teamid'],away_team['teamid'],game_ts)]

	if game_ts > datetime.now().timestamp():
		print("Game is in the future, inserting timestamp only")
		db.insert_data('pfr_gameinfo',data,pk='gameid',columns=['gameid','year','week','home_team','away_team','timestamp'])
		return {"gameinfo":(),"total_offense":()}
		# exit()

	# game info
	gameinfo = scraper.get_gameinfo()
	gameinfo_row = construct_gameinfo_record(gameinfo,year,week)
	if week < 3:
		insert_teams(gameinfo)

	total_offense = scraper.get_total_offense(gameid,db.table_cols['pfr_total_offense'])
	total_offense_data = []
	for to in total_offense:
		total_offense_data.append(construct_pfr_total_offense_record(to))

	data = {
		"gameinfo":gameinfo_row,
		"total_offense":total_offense_data
	}

	scraper.get_pbp(gameid)

	return data

def process_games(year,week,gameid=None):
	games = []
	if gameid != None:
		games = [gameid]
	else:
		sql = f"select gameid from pfr_gameinfo where year = {year} and week = {week}"
		results = db.execute_sql(sql)
		games = []
		for r in results:
			games.append(r['gameid'])
	gameinfo_data = []
	total_offense_data = []
	for g in games:
		print(f"Scraping {g}, {year}-{week}")
		data = scrape_game_data(g,year,week)
		if len(data['gameinfo']) > 0:
			gameinfo_data.append(data['gameinfo'])
		total_offense_data += data['total_offense']

	try:
		if len(gameinfo_data) > 0:
			db.insert_data("pfr_gameinfo",gameinfo_data,pk="gameid")
	except Exception as err:
		print("Error inserting gameinfo for {}, {}".format(year,week))
		print(err)
	try:
		if len(total_offense_data) > 0:
			db.insert_data("pfr_total_offense",total_offense_data,pk="id")
	except Exception as err:
		print("Error inserting total_offense for {}, {}".format(year,week))
		print(err)

def process_commandline():
	# Create the parser
	parser = argparse.ArgumentParser(description="Process some arguments.")

	# Add arguments
	parser.add_argument("year", type=int,help="Year to scrape, required")
	parser.add_argument("week", type=int,help="The week to scrape (optional)")
	parser.add_argument("--gameid", type=str,help="The specific gameid to get data from")

	# Parse the arguments
	args = parser.parse_args()

	# Access arguments
	return args

def main():
	args = process_commandline()
	process_games(args.year,args.week,args.gameid)

if __name__ == '__main__':
	main()
