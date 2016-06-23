class Mongo_Connect(object):

	def __init__(self):
		pass

	def connect(self):
		client = MongoClient('localhost', 27017) #MongoClient(OPENSHIFT_MONGODB_DB_URL)
		return client