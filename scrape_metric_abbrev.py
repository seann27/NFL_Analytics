from PGSQL import NFLDB_SQL
from PFRScraper import PFRScraper, get_soup
import re
import sys

db = NFLDB_SQL()
scraper = PFRScraper()

def main():
	sql = """
		SELECT DISTINCT ON (year) gameid, year, week
		FROM pfr_gameinfo
		WHERE week = 1
		and year in (
			select distinct(gi.year) from pfr_gameinfo gi
			where gi.home_team in
				(
					select teamid from pfr_teams tm
					where tm.metric_abbrev is null
				)
				or gi.away_team in
				(
					select teamid from pfr_teams tm
					 where tm.metric_abbrev is null
				)
			)

		union

		SELECT DISTINCT ON (year) gameid, year, week
		FROM pfr_gameinfo
		WHERE week = 2
		and year in (
			select distinct(gi.year) from pfr_gameinfo gi
			where gi.home_team in
				(
					select teamid from pfr_teams tm
					where tm.metric_abbrev is null
				)
				or gi.away_team in
				(
					select teamid from pfr_teams tm
					 where tm.metric_abbrev is null
				)
			)
	"""

	results = list(db.execute_sql(sql))

	for r in results:
		soup = get_soup(scraper.uri+f"/boxscores/{r['gameid']}",delay=False)
		game_summaries = soup.find("div",{"id":"div_other_scores"})
		atags = game_summaries.findAll("a")
		data = []
		for a in atags:
		    if re.search(r'/teams/\w+/\d+\.htm',a['href']):
		        data.append((a['href'],a.text))
		db.insert_data("pfr_teams",data,pk="teamid",columns=["teamid","metric_abbrev"])

if __name__ == '__main__':
	main()
