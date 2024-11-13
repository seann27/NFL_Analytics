import argparse

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

def main():
	args = process_commandline()
	process_games(args.year,args.week)

if __name__ == '__main__':
	main()
