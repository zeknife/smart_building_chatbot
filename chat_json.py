from llama_cpp import Llama
from agent_json import JsonAgent
import json
import toml
import logging

config = toml.load("config_chat.toml")

# Loads a JSON object consisting of lists of lists of messages
def load_json(file_path):
	try:
		with open(file_path, 'r') as file:
			data = json.load(file)
			print("Loaded example messages from", file_path)
	except Exception as e:
		data = []
		print("Unable to load example messages from", file_path)
		print(e)
	return data

def init_agent(stateful=True, echo=True):
	example_exchanges = load_json(config["examples_path"])
	example_messages = []
	for exchange in example_exchanges:
		example_messages += exchange
	print("Starting chat with example messages:")
	print(example_messages)
	model_path = config["model_dir"] + config["model_filename"]
	print("Initializing model from", model_path, "(First message may take a long time)")
	llm = Llama(model_path=model_path, n_gpu_layers=-1, n_ctx=config["n_ctx"], chat_format=config["chat_format"], verbose=False, offload_kqv=True)
	agent = JsonAgent(llm, messages = example_messages, stateful=stateful, echo=echo)
	return agent

if __name__ == '__main__':
	agent = init_agent()
	while True:
		print()
		user_input = input('User: ')
		agent.respond(user_input)