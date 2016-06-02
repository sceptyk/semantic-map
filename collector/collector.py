import json

class Collector(object):
	'Web API access object'

	def __init__(self, client_key = '', client_secret = '', access_token = '', access_secret = ''):
		self._CLIENT_KEY = client_key
		self._CLIENT_SECRET = client_secret
		self._ACCESS_TOKEN = access_token
		self._ACCESS_SECRET = access_secret

		self.client = self.authorize()

	def authorize(self):
		'Should return client that will be use to connect with API'
		pass

	def process(self, data):
		pass

	def get_data(self):
		'Yield single node data'
		pass
		
	def store_data(self):
		print('Store data')
		with open('tweets.json', 'w') as out:
			data = self.get_data()
			for node in data:
				if getattr(node, "coordinates") is not None:
					out.write(node.to_json())
					out.write('\n')
			out.close()