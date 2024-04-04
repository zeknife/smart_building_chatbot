import pymongo as pm
import datetime
import toml
from json import loads, dumps
from dateparser import parse
from collections import defaultdict
import re
import numpy as np
from pandas import DataFrame

numpy_operators = "min max median std".split()
numpy_functions = {name: getattr(np, name) for name in numpy_operators}

config = toml.load("config_db.toml")
#sensor_config = toml.load("config_sensor.toml")

clientName = config["clientName"]
databaseName = config["databaseName"]
collectionName = config["collectionName"]

# For test db
relative_base = datetime.datetime(2023, 12, 14, 10, 37, 00)

local_db = config["localDB"]

# nominal_ranges = {key: sensor_config[key]['nominal'] for key in sensor_config}
# acceptable_ranges = {key: sensor_config[key]['acceptable'] for key in sensor_config}

# def get_stats(keys, readings):
# 	for 

# For connecting to local DB
def getConnectionInfo(**kwargs):
    # import yaml file
    with open("config.yaml", "r") as file:
        data = yaml.safe_load(file)
    # defaults to local device
    channel = 'local_db'
    data = data[channel]
    # check whether user and pass are present and in correct format
    if(type(data['user']) == type(None) or type(data['pass']) == type(None)):
        clientName = "mongodb://" + str(data['address']) + ":" + str(data['port']) + "/"
    else:
        if(type(data['user']) != str):
            data['user'] = str(data['user'])
        if(type(data['pass']) != str):
            data['pass'] = str(data['pass'])
        clientName = "mongodb://" + data['user'] + ":" + data['pass'] + "@" + str(data['address']) + ":" + str(data['port']) + "/"
    if(type(data['auth_source']) != type(None)):
        clientName = clientName + data['auth_source']
    databaseName = data['database']
    # return the connection information
    return [clientName, databaseName]

def in_range(reading, interval):
	low, high = interval
	return low <= reading < high

# def judgment(readings):
# 	keys = readings[0]['data'].keys()
# 	anomalies = {} # dict of keys 
# 	acceptable = defaultdict(int)
# 	not_acceptable = defaultdict(int)
# 	for reading in readings:
# 		for key, val in reading['data'].items():
# 			if not in_range(val, sensor_config[key]['nominal']):
# 				if in_range(val, sensor_config[key]['acceptable']):
# 					acceptable[key] += 1
# 				else:
# 					not_acceptable[key] += 1
# 	return acceptable, not_acceptable

# def judgment_string()



# Function to parse timedelta string like "1w1d5h23m" to timedelta object
def parse_timedelta_string(time_str):
	weeks = days = hours = minutes = 0
	match = re.match(r'(?:(?P<weeks>\d+)w)?(?:(?P<days>\d+)d)?(?:(?P<hours>\d+)h)?(?:(?P<minutes>\d+)m)?', time_str)
	if match:
		weeks = int(match.group('weeks') or 0)
		days = int(match.group('days') or 0)
		hours = int(match.group('hours') or 0)
		minutes = int(match.group('minutes') or 0)
	return datetime.timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes)
  
# Function to make most queries valid syntax
def remove_text_after_last_parenthesis(input_string):
	last_parenthesis_index = input_string.rfind(')')
	
	if last_parenthesis_index != -1:
		result_string = input_string[:last_parenthesis_index + 1]
		return result_string
	else:
		return input_string

def format_single(reading):
	# now = relative_base
	# result = f"Current time: {now}\n"
	# result += repr(reading)
	result = dumps(reading, indent=4)
	return result


def format_list(readings, keys):
	#Convert list of dicts to dataframe
	df = DataFrame(readings) 
	operators = numpy_operators

	# Convert dataframe to numpy array
	readings_arr = df[keys].to_numpy()
	# print(readings_arr)
	# print("Readings arr shape:", readings_arr.shape)
	# print("Type:", type(readings_arr), "dtype:", readings_arr.dtype)
	# Run each numpy operator on all the data at once for speed (axis 0 ensures that it works for both 1D and 2D arrays)
	statistics = {op: numpy_functions[op](readings_arr, axis=0).round(1) for op in operators}
	statistics_dict = {key: {op: statistics[op][i] for op in operators} for i, key in enumerate(keys)}

	# print("stats dict:", statistics_dict)

	result_dict = {"n_readings": len(readings_arr), "first": readings[0], "last": readings[-1], "stats": statistics_dict}
	for each in ("first", "last"):
		result_dict[each]["created_at"] = result_dict[each]["created_at"].strftime("%Y-%m-%d, %H:%M:%S")

	# print("Result dict:", result_dict)

	# output = f"Got {len(readings)} sensor reading(s):\n"
	output = dumps(result_dict, indent=4)
	# if len(readings) < 4:
	# 	output += "\n".join([str(reading) for reading in readings])
	# else:
	# 	output += "\n".join([str(readings[0]), "...", str(readings[-1])])
	# acceptable, not_acceptable = judgment(readings)
	# print("DEBUG:")
	# print("Acceptable:", acceptable)
	# print("Not acceptable:", not_acceptable)
	return output

# def generate_aggregation_pipeline(sensor_collection, start_time, end_time, keys, operators):
#     match_stage = {
#         '$match': {
#             'created_at': {
#                 '$gte': start_time,
#                 '$lt': end_time
#             }
#         }
#     }
#     group_stage = {'$group': defaultdict(dict)}
#     group_stage['$group']['_id'] = None
    
#     for key in keys:        
#         for operator in operators:
#         	group_stage['$group'][f'{key}.{operator[1:]}'] = {f'{operator}': f'$data.{key}'}

#     pipeline = [match_stage, group_stage]
#     return sensor_collection.aggregate(pipeline)

# def generate_aggregation_pipeline(sensor_collection, start_time, end_time, keys, operators):
#     match_stage = {
#         '$match': {
#             'created_at': {
#                 '$gte': start_time,
#                 '$lt': end_time
#             }
#         }
#     }
    
#     pipeline = [match_stage]
    
#     for key in keys:
#         group_stage = {
#             '$group': {
#                 '_id': f'${key}',  # Grouping by the specific key
#             }
#         }
#         for operator in operators:
#             group_stage['$group'][f'{operator[1:]}_{key}'] = {operator: f'$data.{key}'}
        
#         pipeline.append(group_stage)
    
#     return sensor_collection.aggregate(pipeline)


def queryFromJSON(json_string):
	if local_db: # If local, override clientname and databasename from config_db
		clientNameLocal, databaseNameLocal = getConnectionInfo()
		myClient = pm.MongoClient(clientNameLocal)
		myDatabase = myClient[databaseNameLocal]
	else:
		myClient = pm.MongoClient(clientName)
		myDatabase = myClient[databaseName]
		
	collection = myDatabase[collectionName]

	json_dict = loads(json_string)
	# keys = ['created_at'] + [f"data.{key}" for key in json_dict["keys"]]

	keys = json_dict["keys"]
	# operators = "$first $last $max $min $avg".split() #$stdDevPop
	projection = {f"{key}" : f"$data.{key}" for key in keys}
	projection['_id'] = False
	projection['created_at'] = True

	dt_start = json_dict["dt_start"]
	dt_end = json_dict["dt_end"]
	if not local_db:
		start_time = parse(dt_start, settings={'RELATIVE_BASE': relative_base})
		end_time = parse(dt_end, settings={'RELATIVE_BASE': relative_base})
	else: 
		start_time = parse(dt_start)
		end_time = parse(dt_end)
	# print("Retrieving Keys:", keys)
	# print(type(keys))

	# if "timedelta" in json_dict.keys():
	# 	delta_string = json_dict["timedelta"]
	# 	delta = parse_timedelta_string(delta_string)
	# 	start_time = end_time - delta
	delta = end_time - start_time
	# print("Calculated delta:", delta)
	if delta < datetime.timedelta(minutes=1): # single reading
		reading = collection.find_one({"created_at": {"$lt": end_time}}, projection, sort=[('created_at', pm.DESCENDING)])
		result = format_single(reading)
	else:
		# cursor = generate_aggregation_pipeline(collection, start_time, end_time, keys, operators)
		cursor = collection.find({"created_at": {"$gte": start_time, "$lt": end_time}}, projection)
		readings = list(cursor)
		result = format_list(readings, keys)
		# result = repr(readings)
	# else:
	# 	result = repr(collection.find_one({"created_at": {"$lt": end_time}}))

	return result


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
		result = queryFromJSON(query)
		print("Result:", result)