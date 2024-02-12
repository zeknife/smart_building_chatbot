import pymongo as pm
import datetime
import toml

config = toml.load("config_db.toml")

clientName = config["clientName"]
databaseName = config["databaseName"]
collectionName = config["collectionName"]

# Function to make most queries valid syntax
def remove_text_after_last_parenthesis(input_string):
	last_parenthesis_index = input_string.rfind(')')
	
	if last_parenthesis_index != -1:
		result_string = input_string[:last_parenthesis_index + 1]
		return result_string
	else:
		return input_string

def format_list(readings):
	output = f"Got {len(readings)} sensor readings:\n"
	if len(readings) < 4:
		output += "\n".join([str(reading) for reading in readings])
	else:
		output += "\n".join([str(reading) for reading in readings[:2] + ["..."] + readings[-2:]])
	return output



def querySensorData(query):
	query = remove_text_after_last_parenthesis(query)
	# print("Got db query:", query)
	 
	myClient = pm.MongoClient(clientName)
	myDatabase = myClient[databaseName]
	collection = myDatabase[collectionName]

	filteredData = eval(query, {"sensorData": collection, "datetime": datetime, "pymongo": pm})
	if type(filteredData) in [pm.command_cursor.CommandCursor, pm.cursor.Cursor]:
		filteredData = list(filteredData)
		if len(filteredData) == 1:
			filteredData = filteredData[0]
		elif len(filteredData) > 1:
			filteredData = format_list(filteredData)
	# extractedData = list(filteredData)
	if type(filteredData) is str:
		return filteredData
	else:
		return repr(filteredData)

if __name__ == "__main__":
	print("Running in interactive mode. Enter a pymongo query. Enter a blank message to exit.")
	while True:
		query = input("\nQuery: ")
		if len(query) == 0: break
		result = querySensorData(query)
		print("Result:", result)