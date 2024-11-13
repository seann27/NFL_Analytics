# STARTING FROM SCRATCH
- See the tables and columns in the PGSQL module and create those in postgres except for the pfr_pbp
- Create a wrapper for all the years (in this case, 1994-2024) and run scrape_weekly_links.py for all those years (required for next step)
- Create a wrapper and run scrape_gamelinks.py with the year as input to populate the pfr_gameinfo table with gamelinks
- Create a wrapper and download the HTML pages by running download_weekly_pages.py with the given year and week (implement timer to avoid being rate limited, PFR limits to 60 requests per hour)
- Run scrape_game_wrapper.py to process all available data

# Processing a new week
- Run the donwload_weekly_pages.py script
- Run the scrape_game.py script with year and week as input
