from PGSQL import NFLDB_SQL
from PFRScraper import get_soup, PFRScraper
import re
from tqdm import tqdm
import polars as pl

db = NFLDB_SQL()
scraper = PFRScraper()

def add_positions():
	sql = f"select tot.*,p.* from pfr_total_offense tot, pfr_players p where tot.playerid = p.playerid and tot.position is null"
	results = list(db.execute_sql(sql))
	updates = []
	for r in tqdm(results):
		if r['pos'] == 'FB' or r['pos'] == 'HB':
			r['pos'] = 'RB'
		updates.append((r['id'],r['gameid'],r['playerid'],r['team'],r['pos']))
	if len(updates) > 0:
		db.insert_data("pfr_total_offense",updates,columns=["id","gameid","playerid","team","position"])

# gameid = '202410030atl.htm'
def add_depths(gameid,team):
	cols = db.table_cols['pfr_total_offense'].copy()
	# for idx,c in enumerate(cols):
	# 	if c == 'rush_att':
	# 		cols[idx] = "rush_att+targets as rush_att"
	cols.append("rush_att+targets as flex_att")
	sql = f"select {','.join(cols)} from pfr_total_offense where gameid = '{gameid}' and team = '{team}'"
	# print(sql)
	df = pl.read_database(sql,db.conn)
	positions = {"QB":"pass_att","RB":"flex_att","WR":"targets","TE":"targets"}
	updates = []
	for p,m in positions.items():
		position_df = df.filter(pl.col("position") == p).sort(m,descending=True).with_columns(pl.arange(1, pl.len() + 1).alias("depth"))
		position_df = position_df.drop('flex_att')
		updates += position_df.rows()

	return updates

def add_position_depths():
	sql = "select distinct gameid,team from pfr_total_offense where depth is null"
	results = list(db.execute_sql(sql))
	data = []
	for r in tqdm(results):
		updates = add_depths(r['gameid'],r['team'])
		data += updates

	db.insert_data("pfr_total_offense",data)

def main():
	# sql = """
	# 	SELECT DISTINCT tot.playerid
	# 	FROM pfr_total_offense tot
	# 	LEFT JOIN pfr_players p ON tot.playerid = p.playerid
	# 	WHERE p.playerid IS NULL
	# 	OR p.name IS NULL
	# 	OR p.pos IS NULL
	# """
	sql = """
		SELECT DISTINCT tot.playerid
		FROM pfr_total_offense tot
		LEFT JOIN pfr_players p ON tot.playerid = p.playerid
		WHERE (p.playerid IS NULL OR p.pos IS NULL)
	"""
	results = list(db.execute_sql(sql))
	for r in results:
		name = None
		fullname = None
		pos = None
		try:
			soup = get_soup(scraper.uri+r['playerid'])
			info = soup.find("div",{"id":"info"})
			name = info.find("h1").text.strip()
			meta = info.findAll("p")
			for m in meta:
				search = re.search(r"Position: (\w+)",m.text)
				if search:
					pos = search.group(1).upper()
				else:
					secondary_search = soup.find("td",{"data-stat":"pos"})
					if secondary_search:
						pos = secondary_search.text
			data = [(r['playerid'],name,pos)]
			print((r['playerid'],name,pos))
			db.insert_data("pfr_players",data,pk="playerid",columns=['playerid','name','pos'])
		except Exception as err:
			print(f"Error scraping {scraper.uri+r['playerid']}")
			print(err)
			continue

if __name__ == '__main__':
	main()
	add_positions()
	add_position_depths()
