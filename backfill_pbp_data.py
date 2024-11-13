import upload_weekly_pbp
from PGSQL import NFLDB_SQL
from concurrent.futures import ThreadPoolExecutor, as_completed

db = NFLDB_SQL()

# def run_in_parallel(year, week):
# 	return process_games(year, week)

if __name__ == '__main__':
	sql = "select distinct year, week from pfr_gameinfo order by year asc, week asc"
	results = db.execute_sql(sql)

	# # Use ThreadPoolExecutor to run tasks in parallel
	# with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers based on your system's capacity
	#     futures = [executor.submit(run_in_parallel, r['year'], r['week']) for r in results]
	#
	#     for future in as_completed(futures):
	#         try:
	#             result = future.result()  # Get the result of each process_games call
	#             print(f"Completed task with result: {result}")
	#         except Exception as e:
	#             print(f"Task raised an exception: {e}")
	for r in results:
		upload_weekly_pbp.main(r['year'],r['week'])
