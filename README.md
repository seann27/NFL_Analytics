# NFL Analytics

> **IMPORTANT DISCLAIMER:** Setup and installation instructions in this document are currently incomplete and will be fully documented in the near future. Please check back for updates.

A comprehensive NFL statistics pipeline and analysis tool that retrieves, processes, and analyzes game data, storing it in a PostgreSQL database for matchup analysis and player performance tracking.

## Features

- Automated retrieval of NFL game statistics using Selenium and BeautifulSoup4
- Detailed play-by-play parsing with component breakdown
- Player position and depth chart tracking
- Performance metrics calculation comparing recent game performance to rolling averages
- Matchup analysis capabilities for upcoming games
- Historical data coverage from 1994 onwards
- Weekly data pipeline for keeping statistics current

## Prerequisites

- PostgreSQL database
- Python 3.x
- Firefox web browser
- Approximately 2GB of storage space for game data

## Required Python Packages

```
psycopg2
beautifulsoup4
selenium
pandas
numpy
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/NFL_Analytics.git
cd NFL_Analytics
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Configure your PostgreSQL database:
   - Create a new database
   - Update the database configuration in `config.py` with your credentials
   - Run the database initialization scripts (see Database Setup section)

4. Ensure Firefox is installed on your system (required for Selenium)

## Usage

### Running the Weekly Pipeline

To retrieve and process data for a specific week:

```bash
python launch_weekly_pipeline.py <year> <week>
```

Example for Week 10 of 2024:
```bash
python launch_weekly_pipeline.py 2024 10
```

### Getting Upcoming Matchup Analysis

1. Run the weekly pipeline for the upcoming week
2. Access the generated Excel report in the `reports` directory
3. Review player and position matchup statistics for upcoming games

## Data Pipeline

The pipeline performs the following steps:

1. Retrieves game summary data using Selenium web scraping
2. Parses play-by-play data with custom component analysis
3. Calculates player positions and depth charts
4. Processes performance metrics:
   - Rolling 5-game average performance
   - Position-specific metrics
   - Matchup-based analysis
5. Stores processed data in PostgreSQL database
6. Generates analysis reports

## Database Structure

Detailed database schema and structure documentation to be provided. Key aspects include:

- Game summary tables
- Play-by-play breakdown
- Player position tracking
- Performance metrics
- Historical statistics

## Backfilling Historical Data

For optimal performance metrics, it's recommended to backfill at least the current season's data. Detailed backfilling instructions will be provided in a separate document.

## Performance Metrics

The system calculates several performance metrics:

- Individual player performance vs. rolling average
- Position group performance trends
- Matchup-specific statistics
- Opponent strength adjustments

## Contributing

Please read CONTRIBUTING.md for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the LICENSE.md file for details

## Acknowledgments

- NFL statistics providers
- Open-source community contributions
- Database optimization techniques from the community

## Support

For support and questions, please open an issue in the repository.
