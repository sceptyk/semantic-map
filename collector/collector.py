from mysql_connect import Mysql_Connect

class Collector(object):
	'Web API access object'

	def __init__(self, client_key = '', client_secret = '', access_token = '', access_secret = '', dbuser = 'user', dbpass = ''):
		self._CLIENT_KEY = client_key
		self._CLIENT_SECRET = client_secret
		self._ACCESS_TOKEN = access_token
		self._ACCESS_SECRET = access_secret

		self.client = self.authorize()
		self.db = Mysql_Connect()		

	def authorize(self):
		'Should return client that will be used to connect with API'
		pass

	def process(self, data):
		'Process raw data to get node data'
		pass

	def get_data(self):
		'Yield single node data'
		pass
		
	# @param {iterable} data
	def store_data(self, data):
		'Stores single node data,'
		pass

	def run(self):
		'Runs the collector'
		pass