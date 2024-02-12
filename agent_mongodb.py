from chatformat import format_chat_prompt
# from pandasql import sqldf
from json import loads, dump
from database import querySensorData

def save_json(my_dict, path):
	with open(path, 'w') as json_file:
		dump(my_dict, json_file, indent=4)

sysprompt = """You are a conversational agent for a smart building. You have access to a MongoDB time series collection called sensorData, containing documents called "data" and timestamps called "created_at". Answer questions from the user by writing PyMongo queries to retrieve relevant information.
Keys in "data": 
[temp: Temperature (Celsius)
hum: Humidity (Percent)
tvoc: Total Volatile Organic Compounds concentration (ppm)
co2: Carbon Dioxide concentration (ppm)
pm0_3: Particulate Matter (PM) <= 0.3 micrometers (ppm)
pm0_5: Particulate Matter (PM) <= 0.5 micrometers (ppm)
pm1_0: Particulate Matter (PM) <= 1.0 micrometers (ppm)
pm2_5: Particulate Matter (PM) <= 2.5 micrometers (ppm)
pm10: Particulate Matter (PM) <= 10 micrometers (ppm)
nh3: Ammonia concentration (ppm)
no2: Nitrogen Dioxide concentration (ppm)
co: Carbon Monoxide concentration (ppm)
c2h5oh: Ethanol concentration (ppm)
h2: Hydrogen concentration (ppm)
ch4: Methane concentration (ppm)
c3h8: Propane concentration (ppm)
c4h10: Butane concentration (ppm)
press: Atmospheric pressure (hpa)
sound_level: Sound level or intensity (db)]

Do not answer any questions unrelated to the sensor readings.
If no database query is necessary, answer directly. If the user's request is ambiguous or unclear, ask them to specify. Otherwise, answer data-oriented questions using the following format:
Plan: Specific plan for what the MongoDB query should accomplish
Query: A single PyMongo python query for retrieving relevant information. Remember that PyMongo returns an iterator, and you need to convert it to a list or similar.
Result:
(Result of the PyMongo query)
Answer: Concise and comprehensive answer to the user's question. If the retrieved data is not adequate to answer the question, or there are any other anomalies, inform the user in a cooperative manner. Keep in mind that the user cannot see the SQL query or the retrieved data, only your written answer."""


class MongoDBAgent:
	def __init__(self, llm, chat_format = 'chatml', system_prompt = sysprompt, messages = [], query_stop = "Result:", stateful=True, echo=True):
		self.llm = llm
		# self.dataframe = dataframe
		self.chat_format = chat_format
		self.system_prompt = system_prompt
		self.messages = [self._genMessage(system_prompt, role='system')] + messages
		self.stateful = stateful # Whether to save interactions to the message log
		self.query_stop = query_stop
		self.echo = echo
		# print("Agent initialized with messages:")
		# print(self.messages)

	def _genMessage(self, content, role='user'):
		return {"role": role, "content": content}

	# def pysqldf(query): return sqldf(q, globals())


	def respond(self, user_input):
		user_message = self._genMessage(user_input)
		messages_temp = self.messages + [user_message]
		choice = self.llm.create_chat_completion(messages_temp, temperature=0, max_tokens=512, stop=[self.query_stop])['choices'][0]
		agent_message = choice['message']
		messages_temp.append(agent_message)
		content = agent_message['content']
		if self.echo: print("Assistant: " + content)

		if 'Query: ' in content:
			query = content.split('Query: ')[1].rstrip('.\n\\')
			# print("GENERATED QUERY:", query)
			try:
				# df = self.dataframe
				# query_doc = loads(split[1])
				result = querySensorData(query)
				# result = sqldf(query, locals())
			except Exception as e:
				result = repr(e)
			# print("Full log:")
			# print(messages_temp)
			try:
				prompt, stop = format_chat_prompt(self.chat_format, messages_temp)
			except KeyError:
				save_json(messages_temp, "error_messages.json")
				print("DUMPED ERRORED MESSAGE TO FILE")
				return "Error"

			# print("TYPE:", type(result))
			append = "Result:\n" + result + '\nAnswer:'
			if self.echo: print(append, end="")
			prompt += append
			completion = self.llm.create_completion(prompt, max_tokens=256, temperature=0, stop=[stop])
			answer = completion['choices'][0]['text']
			# usage = completion['usage']
			# print("Usage:", usage)
			messages_temp[-1]['content'] += append + answer
			if self.echo: print(answer)
			# print("Full message:", messages_temp[-1]['content'])

		if self.stateful:
			self.messages = messages_temp

		return agent_message
