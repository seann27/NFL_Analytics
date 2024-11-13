from PGSQL import NFLDB_SQL
from PFRScraper import PFRScraper
import sys

db = NFLDB_SQL()
scraper = PFRScraper()

def main(year):
	summary = scraper.get_weekly_links(year)
	data = []
	for wl,meta in summary.items():
		data.append((wl,meta['year'],meta['week'],meta['is_postseason']))
	db.insert_data('pfr_weekly_links',data,pk='link')

if __name__ == '__main__':
	year = sys.argv[1]
	main(year)
