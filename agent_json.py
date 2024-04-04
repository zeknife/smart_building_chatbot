# from chatformat import format_chat_prompt
# from pandasql import sqldf
from json import loads, dump
from database_json import querySensorData, queryFromJSON
# from dateutil import parser
from llama_cpp import llama_grammar

def save_json(my_dict, path):
	with open(path, 'w') as json_file:
		dump(my_dict, json_file, indent=4)

result_role = 'user'

plan_grammar_string = """root ::= "Plan:" rest

rest ::= CHAR | CHAR rest

CHAR ::= [a-zA-Z0-9 !"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~]"""

answer_grammar_string = """root ::= "Answer:" rest

rest ::= CHAR | CHAR rest

CHAR ::= [a-zA-Z0-9 !"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~]"""

# answer_grammar_string """root   ::= answer
# answer ::= "Answer:" ws any_text ws

# any_text ::=
#   ( CHAR | ws )*

# ws ::= [ \t\n\r]*

# CHAR ::= [\x20-\x7E]
# """


plan_grammar = llama_grammar.LlamaGrammar.from_string(plan_grammar_string, verbose=False)
answer_grammar = llama_grammar.LlamaGrammar.from_string(answer_grammar_string, verbose=False)

sysprompt = """
You are a conversational agent for a smart building. You have access to a collection of sensor data. Answer questions from the user by using JSON objects to retrieve relevant information from the appropriate time interval. Make sure to always retrieve relevant data before answering questions about it.
Keys in data: 
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

JSON query format:
{
	"keys": (list of keys that should be retrieved),
	"dt_start": (Datetime to retrieve from, examples: 2023-02-22, 2 weeks ago, 30 min ago),
	"dt_end": (Datetime to retrieve until, examples: 2024-01-01, November 2023, Now)
}
Example:
{
	"keys": ["hum", "temp", "press"],
	"dt_start": "5 min ago"
	"dt_end": "now"
}

Do not answer any questions unrelated to the sensor readings.
If no database query is necessary, answer directly. If the user's request is ambiguous or unclear, ask them to specify. Otherwise, answer data-oriented questions using the following format:
Plan: Specific plan for what data should be retrieved, and from what time interval. If no database query is needed, answer directly.
Query: JSON object in the format described above.
Then the user will provide the readings from the database.
Finally, answer the original question in a new message like this:
Answer: Concise but comprehensive answer to the user's question. If the retrieved data is not adequate to answer the question, or there are any other anomalies, inform the user in a cooperative manner. Keep in mind that the user cannot see the JSON query or the retrieved data, only your written answer.
"""

# Function to get rid of text after json query
def remove_text_after_last_bracket(input_string):
	index = input_string.rfind('}')
	
	if index != -1:
		result_string = input_string[:index + 1]
		return result_string
	else:
		return input_string


class JsonAgent:
	def __init__(self, llm, chat_format = 'chatml', system_prompt = sysprompt, messages = [], query_stop = ["\nAnswer:", "\n<|"], stateful=True, echo=True): # [, "\n\{"]
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

	def get_latest_readings(self):
		return "Latest sensor readings:\n" + querySensorData("sensorData.find_one()")
	# def pysqldf(query): return sqldf(q, globals())


	def respond(self, user_input):
		user_message = self._genMessage(user_input)
		# sensor_message = self._genMessage(self.get_latest_readings(), role='context')
		# print("System:", sensor_message['content'])
		messages_temp = self.messages + [user_message]
		json_grammar = llama_grammar.LlamaGrammar.from_string(llama_grammar.JSON_GBNF, verbose=False) #Need to do this instead of using response_format to set verbose to False
		# choice = self.llm.create_chat_completion(messages_temp, temperature=0, max_tokens=512, stop=self.query_stop, response_format={"type": "json_object"})['choices'][0]
		choice = self.llm.create_chat_completion(messages_temp, top_k=1, max_tokens=512, stop=self.query_stop)['choices'][0]
		agent_message = choice['message']
		messages_temp.append(agent_message)
		content = agent_message['content']
		if self.echo: print(f"{agent_message['role']}: {content}")
		# print("content length:", len(content))

		if 'Query: ' in content:
			content = remove_text_after_last_bracket(content)
			query = content.split('Query: ')[1]#.rstrip('.\n\\')
			# print("GENERATED QUERY:", query)
			# try:
				# df = self.dataframe
				# query_doc = loads(split[1])
			result = queryFromJSON(query)
				# result = sqldf(query, locals())
			# except Exception as e:
			# 	result = repr(e)
			# print("Full log:")
			# print(messages_temp)
			# try:
			# 	prompt, stop = format_chat_prompt(self.chat_format, messages_temp)
			# except KeyError:
			# 	save_json(messages_temp, "error_messages.json")
			# 	print("DUMPED ERRORED MESSAGE TO FILE:")
			# 	print(messages_temp)
			# 	return "Error"
			result_message = self._genMessage(result, role=result_role)
			messages_temp.append(result_message)
			# messages_temp.append(self._genMessage("Use the above information to answer my last question.", role='user'))
			if self.echo: print(f"{result_message['role']}: {result_message['content']}")

			# print("TYPE:", type(result))
			# append = "Result:\n" + result + '\nAnswer:'
			# print("FULL TEMP MESSAGE LOG:")
			# for message in messages_temp: print(message)
			answer_choice = self.llm.create_chat_completion(messages_temp,  top_k=1, max_tokens=512, grammar = answer_grammar)['choices'][0]
			answer_message = answer_choice['message']
			answer_content = answer_message['content']
			messages_temp.append(answer_message)
			if self.echo: print(f"{answer_message['role']}: {answer_content}")
			# prompt += append
			# completion = self.llm.create_completion(prompt, max_tokens=256, temperature=0, stop=[stop])
			# answer = completion['choices'][0]['text']
			# usage = completion['usage']
			# print("Usage:", usage)
			# messages_temp[-1]['content'] += append + answer
			# if self.echo: print(answer)
			# print("Full message:", messages_temp[-1]['content'])

		if self.stateful:
			self.messages = messages_temp

		return messages_temp[2:]
