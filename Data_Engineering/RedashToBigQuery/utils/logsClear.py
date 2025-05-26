import json 
import regex as re

with open("temp/errorAnalysis.json", "r") as f:
    errorAnalysis = json.load(f)
    missingTables = errorAnalysis["TableNotFoundError"]["errorMessage"]
    missingTables = [re.search(r"dl_(\w+)", table).group(1) for table in missingTables]
    errorAnalysis["TableNotFoundError"]["errorMessage"] = list(set(missingTables))

    print("Missing tables:")
    for table in errorAnalysis["TableNotFoundError"]["errorMessage"]:
        print(table)

with open("temp/errorAnalysis.json", "w") as f:
    json.dump(errorAnalysis, f, indent=4)
