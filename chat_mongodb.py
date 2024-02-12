from llama_cpp import Llama
from agent_mongodb import MongoDBAgent
import json
import toml

config = toml.load("config_chat.toml")

# Loads a JSON object consisting of lists of lists of messages
def load_json(file_path):
	try:
		with open(file_path, 'r') as file:
			data = json.load(file)
			print("Loaded example messages from", file_path)
	except:
		data = []
		print("Unable to load example messages from", file_path)
	return data


example_exchanges = load_json(config["examples_path"])
example_messages = []
for exchange in example_exchanges:
	example_messages += exchange
# print("Starting chat with example messages:")
# print(example_messages)
print("Loading model", config["model_path"])
llm = Llama(model_path=config["model_path"], n_gpu_layers=-1, n_ctx=config["n_ctx"], chat_format=config["chat_format"], verbose=False, offload_kqv=True)
agent = MongoDBAgent(llm, messages = example_messages)

print()
while True:
	user_input = input('User: ')
	agent.respond(user_input)