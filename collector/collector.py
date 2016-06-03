import MySQLdb

class Collector(object):
	'Web API access object'

	def __init__(self, client_key = '', client_secret = '', access_token = '', access_secret = '', dbuser = 'user', dbpass = 'pass', ):
		self._CLIENT_KEY = client_key
		self._CLIENT_SECRET = client_secret
		self._ACCESS_TOKEN = access_token
		self._ACCESS_SECRET = access_secret

		self.client = self.authorize()
		self.conn = MySQLdb.connect('localhost', dbsuer, dbpass, 'semantic_map_2016')
		self.cursor = self.conn.cursor()		

	def authorize(self):
		'Should return client that will be used to connect with API'
		pass

	def process(self, data):
		'Process raw data to get node data'
		pass

	def get_data(self):
		'Yield single node data'
		pass
		
	def store_data(self, data):
		'Stores single node data'
		pass