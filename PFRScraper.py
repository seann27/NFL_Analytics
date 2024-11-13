# imports
import psycopg2
import os
import sys
import traceback
from datetime import datetime
import pytz
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from webdriver_manager.firefox import GeckoDriverManager
import pandas as pd
import numpy as np
import re
from bs4 import Comment
from urllib.request import urlopen as uReq
import random
import requests
import time
import pickle
from glob import glob
import polars as pl
from PGSQL import NFLDB_SQL

db = NFLDB_SQL()

# List of user agents
user_agents = [
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
	'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1 Safari/605.1.15',
	'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
	'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
]

def get_soup(url,delay=60):
	# Select a random user agent
	# user_agent = random.choice(user_agents)
	# headers = {'User-Agent': user_agent}

	# uClient = uReq(url)
	# page_html = uClient.read()
	# uClient.close()
	# Make the request using the selected user agent
	# return BeautifulSoup(page_html, "html.parser")

	# response = requests.get(url, headers=headers)
	options = Options()
	options.headless = True
	options.add_argument('--blink-settings=imagesEnabled=false')
	# options.add_argument('--ignore-certificate-errors')
	# options.add_argument('--disable-proxy-certificate-handler')
	options.binary_location = r"C:\Program Files\Mozilla Firefox\firefox.exe"
	# options.add_experimental_option('excludeSwitches', ['enable-logging'])
	driver=webdriver.Firefox(options=options,service=Service(GeckoDriverManager().install()))
	driver.get(url)
	html = driver.page_source
	driver.close()
	if delay:
		time.sleep(delay)
	return BeautifulSoup(html, "html.parser")

def get_comment_soup(soup):
	# Assuming `soup` is your original BeautifulSoup object
	comments = soup.find_all(string=lambda text: isinstance(text, Comment))

	# Initialize an empty string to store all the combined HTML from comments
	combined_comments_html = ''

	if comments:
		for c in comments:
			comment_soup = BeautifulSoup(c, 'html.parser')
			combined_comments_html += str(comment_soup)  # Convert each comment_soup to string and concatenate

	# Now parse the combined HTML into one large BeautifulSoup object
	combined_soup = BeautifulSoup(combined_comments_html, 'html.parser')
	return combined_soup

def get_downloaded_page(url):
	gameid = os.path.basename(url).replace(".htm","")
	search_link = os.path.normpath(f"gamelinks_content/*{gameid}*")
	filesearch = glob(search_link)
	if len(filesearch) != 1:
		print(f"Error searching for {search_link}")
		exit()
	with open(filesearch[0],'rb') as pickle_file:
		page_data = pickle.load(pickle_file)
	page_source = page_data['scorebox']+page_data['gameinfo']

	apo = BeautifulSoup(page_data['all_player_offense'],"html.parser")
	if apo.find("div",{"id":"all_player_offense"}) is None:
		page_data['all_player_offense'] = f"<div id=all_player_offense>{page_data['all_player_offense']}</div>"

	pbp = BeautifulSoup(page_data['all_plays'],"html.parser")
	if pbp.find("div",{"id":"div_pbp"}) is None:
		page_data['all_plays'] = f"<div id=div_pbp>{page_data['all_plays']}</div>"

	page_source += page_data['all_player_offense']
	page_source += page_data['all_plays']
	return BeautifulSoup(page_source,"html.parser")

def search_stat(row,tag,stat):
	search = row.find(tag,{"data-stat":stat})
	if search:
		return search.text
	else:
		return None

class PFRScraper:

	def __init__(self):
		self.uri = "https://www.pro-football-reference.com"

	def load_soup(self,url,preloaded=True,delay=60):
		self.url = url
		if preloaded == False:
			time.sleep(5)
			self.soup = get_soup(url,delay=delay)
		else:
			self.soup = get_downloaded_page(url)

		self.comment_soup = get_comment_soup(self.soup)

	def get_weekly_links(self,year):
		uri = "https://www.pro-football-reference.com"
		url = "{}/years/{}".format(uri,year)
		self.load_soup(url)
		# soup = get_soup(url)
		soup = self.soup
		gamelinks = {}
		summary = {}
		div = soup.find("div",{"id":"div_week_games"})
		if div is None:
			# soup = get_comment_soup(soup)
			soup = self.comment_soup
		div = soup.find("div",{"id":"div_week_games"})
		if div:
			links = div.findAll("a")
			for link in links:
				gamelinks[link.get('href')] = link.text
		else:
			print("Unable to parse weekly links")
			return summary

		for g,n in gamelinks.items():
			s = re.search(r'week_(\d+)\.htm$',g)
			if s:
				week = int(s.group(1))
			is_postseason = False
			s = re.search(r'week',n.lower())
			if s is None:
				is_postseason = True
			summary[uri+g] = {"year":year,"week":week,"is_postseason":is_postseason}
		return summary

	def get_game_links(self,url=None):
		# soup = get_soup(url)
		if url != None:
			self.load_soup(url,preloaded=False)
		soup = self.soup
		links = soup.find("div",{"id":"content"}).findAll("td",{"class":"gamelink"})
		gamelinks = []
		for l in links:
			gamelinks.append(l.a['href'])
		return gamelinks

	def get_game_timestamp(self,soup=None):
		soup = self.soup
		if soup != None:
			soup = soup
		divs = soup.find("div",{"class":"scorebox"}).find("div",{"class":"scorebox_meta"}).findAll("div")
		# Regex patterns to extract date and time
		date_pattern = r'[A-Za-z]+\s[A-Za-z]{3}\s\d{1,2},\s\d{4}'
		time_pattern = r'Start Time:\s*(\d{1,2}:\d{2}[ap]m)'
		date_str = None
		time_str = None
		for d in divs:
			# Extract date and time from the input list
			date_search = re.search(date_pattern, d.text)
			if date_search:
				date_str = date_search.group(0)
			time_search = re.search(time_pattern,d.text)
			if time_search:
				time_str = time_search.group(1)
		timestamp = None
		if date_str and time_str:
			# Combine date and time into a single string
			datetime_str = f"{date_str} {time_str}"

			# Parse the combined datetime string
			datetime_obj = datetime.strptime(datetime_str, "%A %b %d, %Y %I:%M%p")

			# Set the timezone to Eastern Time
			eastern_tz = pytz.timezone('US/Eastern')
			eastern_time = eastern_tz.localize(datetime_obj)

			# Convert to Unix timestamp
			timestamp = int(eastern_time.timestamp())
		return timestamp

	def get_teams(self,soup=None):
		soup = self.soup
		if soup != None:
			soup = soup

		gameteams = soup.find("div",{"class":"scorebox"})
		atags = gameteams.findAll("a")
		teams = []
		for a in atags:
			s = re.search(r'/teams/(\w+)/\d+\.htm',a['href'])
			if s:
				teams.append({"teamid":a['href'],"abbrev":s.group(1).upper(),"name":a.text})
		# if week is 1, insert teams into team database
		home_team = teams[1]
		away_team = teams[0]

		return home_team,away_team,gameteams

	def get_gameinfo(self,url=None):
		try:
			if url != None:
				self.load_soup(url)
			soup = self.soup

			gameinfo = {
				"gameid":'',
				"home_team":{},
				"away_team":{},
				"timestamp":'',
				"home_odds":0,
				"vegas_total":40
			}

			gameinfo['gameid'] = os.path.basename(self.url)

			# gameteams = soup.find("div",{"class":"scorebox"})
			# atags = gameteams.findAll("a")
			# teams = []
			# for a in atags:
			# 	s = re.search(r'/teams/(\w+)/\d+\.htm',a['href'])
			# 	if s:
			# 		teams.append({"teamid":a['href'],"abbrev":s.group(1).upper(),"name":a.text})
			# # if week is 1, insert teams into team database
			# home_team = teams[0]
			# away_team = teams[1]
			home_team,away_team,gameteams = self.get_teams()

			scores = gameteams.findAll("div",{"class":"score"})
			home_team['score'] = scores[0].text
			away_team['score'] = scores[1].text

			logos = []
			images = gameteams.findAll("img",{"class":"teamlogo"})
			for i in images:
				logos.append(i['src'])

			home_team['logo'] = logos[0]
			away_team['logo'] = logos[1]

			records = []
			divs = gameteams.findAll("div")
			for d in divs:
				if d.text:
					if re.search(r'^(\d+)-(\d+)',d.text):
						records.append(d.text)
			home_record = records[0].split('-')
			away_record = records[1].split('-')
			home_team['ties'] = 0
			away_team['ties'] = 0
			if len(home_record) > 2:
				home_team['ties'] = home_record[2]
			if len(away_record) > 2:
				away_team['ties'] = away_record[2]
			home_team["wins"] = int(home_record[0])
			home_team["losses"] = int(home_record[1])
			away_team["wins"] = int(away_record[0])
			away_team["losses"] = int(away_record[1])

			gameinfo["home_team"] = home_team
			gameinfo["away_team"] = away_team
			gameinfo["timestamp"] = self.get_game_timestamp(soup)

			game_summary = soup.find("div",{"id":"game_info"})
			if game_summary is None:
				# comment_soup = get_comment_soup(soup)
				# game_summary = comment_soup.find("table",{"id":"game_info"})
				game_summary = soup.find("table",{"id":"game_info"})
			if game_summary:
				rows = game_summary.findAll("tr")
				for r in rows:
					label = r.find("th",{"data-stat":"info"})
					team_pattern = r"([a-zA-Z\s]+)\s(?:49ers\s)?(-?\d+(\.\d+)?)"
					total_pattern = r"(-?\d+(\.\d+)?)"
					if label:
						if re.search(r'Vegas Line',label.text):
							stat = r.find("td")
							# Extract team name and number from the team line
							team_match = re.match(team_pattern, stat.text)
							if team_match:
								team_name = team_match.group(1).strip()
								team_number = float(team_match.group(2))
								if team_name != home_team['name']:
									team_number *= -1
								gameinfo['home_odds'] = team_number
							elif re.match(r'Pick',stat.text):
								gameinfo['home_odds'] = 0

						if re.search(r'Over/Under',label.text):
							stat = r.find("td")
							# Extract number from the total line
							total_match = re.match(total_pattern, stat.text)
							if total_match:
								total_number = total_match.group(1)
								gameinfo['vegas_total'] = float(total_number)

			return gameinfo
		except Exception as err:
			print("Error scraping {}".format(self.url))
			print(err)

	def get_total_offense(self,gameid,data_stats,url=None):
		try:
			if url != None:
				self.load_soup(url)
			data = self.soup.find("div",{"id":"all_player_offense"})
			rows = data.findAll("tr")

			datarows = []
			for r in rows:
				# initialize data
				data = {}
				for col in data_stats:
					data[col] = None

				player = r.find("th",{"data-stat":"player","scope":"row"})
				if player:
					data['id'] = "{}-{}".format(gameid,player.a['href'])
					data['gameid'] = gameid
					data['playerid'] = player.a['href']
				else:
					continue

				for dt in data_stats[3:]:
					stat = r.find("td",{"data-stat":dt})
					if stat:
						if stat.text == '':
							data[dt] = '0'
						else:
							data[dt] = stat.text
					else:
						data[dt] = '0'
				datarows.append(data)
			return datarows
		except Exception as err:
			print(err)
			return []

	def process_play(self,gameid,row):
		row_data = {}
		for k in db.table_cols['pfr_pbp']:
			row_data[k] = None

		row_data['id'] = f"{gameid}-{row['data-row']}"
		if row.has_attr("class") and "divider" in row['class']:
			row_data['possession_change'] = True
		else:
			row_data['possession_change'] = False
		row_data['quarter'] = search_stat(row,"th","quarter")
		row_data['qtr_time_remain'] = search_stat(row,"td","qtr_time_remain")
		row_data['down'] = search_stat(row,"td","down")
		row_data['yds_to_go'] = search_stat(row,"td","yds_to_go")
		row_data['location'] = search_stat(row,"td","location")
		row_data['pbp_score_aw'] = search_stat(row,"td","pbp_score_aw")
		row_data['pbp_score_hm'] = search_stat(row,"td","pbp_score_hm")
		row_data['exp_pts_before'] = search_stat(row,"td","exp_pts_before")
		row_data['exp_pts_after'] = search_stat(row,"td","exp_pts_after")

		detail = row.find("td",{"data-stat":"detail"})
		detail_text = detail.text
		atags = detail.findAll("a")
		player_map = {}
		for a in atags:
			if a.has_attr("href") and re.search(r'/players/',a['href']):
				# detail_text = detail_text.replace(a.text,f"{a.text}:[{a['href']}]")
				player_map[a.text] = f"{a.text}:[{a['href']}]"
		for pid,name in player_map.items():
			detail_text = detail_text.replace(pid,name)
		row_data['detail'] = detail_text

		return row_data

	def get_pbp(self,gameid,url=None):
		if url != None:
			self.load_soup(url)
		soup = self.soup.find("div",{"id":"div_pbp"})
		rows = soup.findAll("tr")

		# initialize pbp data structure
		pbp_data = {}
		for col in db.table_cols['pfr_pbp']:
			pbp_data[col] = []

		for r in rows:
			search = r.find("th",{"scope":"row"})
			if search:
				row_data = self.process_play(gameid,r)
				for k,v in row_data.items():
					pbp_data[k].append(v)

		df = pl.from_dict(pbp_data)
		filepath = os.path.normpath(f"pbp_data/{gameid.replace('.htm','')}_pbp.parquet")
		df.write_parquet(filepath)
		print(f"File written: {filepath}")
