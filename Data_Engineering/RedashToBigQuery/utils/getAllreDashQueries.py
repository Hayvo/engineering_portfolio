from utils.reDashHandler import ReDashHandler

redashHandler = ReDashHandler()
queries = redashHandler.getAllQueries()
print(f"Total queries: {len(queries)}")
for query in queries:
    redashHandler.saveJsonQuery(query)