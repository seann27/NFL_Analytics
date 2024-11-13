from PGSQL import NFLDB_SQL
import PFRScraper
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from webdriver_manager.firefox import GeckoDriverManager
import re
import csv
import time
import pickle
import os

db = NFLDB_SQL()
scraper = PFRScraper.PFRScraper()

def get_filename(gameid):
	gametype = "REG"
	sql = f"""select wl.is_postseason as is_postseason, wl.year as year, wl.week as week
			  from pfr_weekly_links wl, pfr_gameinfo gi
			  where wl.year = gi.year
			  and wl.week = gi.week
			  and gi.gameid = '{gameid}'"""
	result = list(db.execute_sql(sql))
	if len(result) == 0:
		print("Error! Gametype label query in download_gamepage returned no results!")
		exit()
	if result[0]['is_postseason']:
	    gametype = "POST"

	return f"{gametype}_{result[0]['year']}_{result[0]['week']}_{gameid.replace('.htm','')}.pkl"

def download_soup(gameid,delay=5):
	try:
		soup = PFRScraper.get_soup(scraper.uri+f"/boxscores/{gameid}",delay=delay)
		content = {
			'scorebox':None,
			'gameinfo':None,
			'all_player_offense':None,
			'all_plays':None
		}
		# content['scorebox'] = str(soup.find("div",{"class":"scorebox"}))
		# content['gameinfo'] = str(soup.find("div",{"id":"all_game_info"}).find("table"))
		# content['all_player_offense'] = str(soup.find("div",{"id":"all_player_offense"}).find('tbody').findAll("tr"))
		# content['all_plays'] = str(soup.find("div",{"id":"div_pbp"}).find("tbody").findAll("tr"))
		content['scorebox'] = str(soup.find("div",{"class":"scorebox"}))
		content['gameinfo'] = str(soup.find("div",{"id":"all_game_info"}))
		content['all_player_offense'] = str(soup.find("div",{"id":"all_player_offense"}))
		content['all_plays'] = str(soup.find("div",{"id":"div_pbp"}))

		filename = get_filename(gameid)
		with open(os.path.join("gamelinks_content",filename),"wb") as pickle_file:
			pickle.dump(content,pickle_file)
		pickle_file.close()
		print(f"Data wrote to gamelinks_content/{filename}\n")
	except Exception as err:
		print(f"Error when downloading {gameid}")
		print(err)
