from model.tweet import Tweet
from collector.mysql_connect import Mysql_Connect
from util.util import Util
import time
import string
import os

class Cloud_Parser(object):
	"""Parse collected data, retrieve keywords and store them"""
	def __init__(self):
		self.conn = Mysql_Connect()
		self.util = Util()

		self.stopwords = self._get_stopwords()

	def get_data(self):
		chunk_size = 500
		start_time = self._get_last_twt_kword("2000-01-01 00:00:00")

		query = """SELECT * FROM tweets WHERE timestamp > '%s' ORDER BY timestamp ASC LIMIT %d"""

		while True:
			
			try:
				cursor = self.conn.execute(query % (start_time, chunk_size))
				
				if cursor.rowcount == 0:
					print "Not enough tweets - sleep 1hr"
					time.sleep(3600)
					continue

				results = cursor.fetchall()

				twt = Tweet()
				for row in results:
					twt.populate(row)
					try:
						self.store_data(twt)
					except Exception, e:
						print(str(e))
						continue
				start_time = twt.dict()['time']

				break
			except Exception, e:
				print(str(e))

#PARSE START
	def _parse_timestamp(self, timestamp):
		return timestamp.strftime('%H:%M:%S'), int(timestamp.strftime('%w'))+1

	def _get_keywords(self, txt):
		#clear and split the text
		list = txt.translate(string.maketrans("", ""), string.punctuation).split(' ')
		#return just meaningful words
		return [w for i, w in enumerate(list) if not (w.lower() in self.stopwords
			or len(w) <= 2 
			or any(ch.isdigit() for ch in w)
			or w.startswith('http'))]

	def _get_stopwords(self):
		with open(os.path.normpath("processor/stopwords.txt")) as input:
			text = input.readline()

		return text.split(", ")

#INSERTS START
	def insert_keywords(self, list):
		query = """INSERT IGNORE INTO keywords (word) VALUES %s"""
		values = ""

		for w in list:
			values += "('%s'), " % w.lower()
		
		self.conn.execute(query % values[:-2]) #remove the last coma

	def insert_counters(self, keywords, lat, lng, date):
		query = """INSERT INTO word_counter (_keyword, _cloud, _layer, day_time, day) 
			VALUES %s ON DUPLICATE KEY UPDATE count = count + 1"""
		
		for k in keywords:
			values = ""
			for layer in range(1, 5):
				values += "((SELECT _id FROM keywords WHERE word = '%s'), '%s', %d, %d, %d), " % (k, self.util.hash_geo(lat, lng, self.util.layer_precision(layer)), layer, self.util.day_time(date[0]), date[1])

			self.conn.execute(query % values[:-2]) #remove the last coma
			
	def insert_twt_kwords(self, keywords, tweet_id):
		delete = """DELETE tk.* FROM tweet_keywords tk
			INNER JOIN tweets t ON tk._tweet = t._id
			WHERE t.timestamp < DATE_SUB(NOW(), INTERVAL 2 WEEK)"""

		insert = """INSERT INTO tweet_keywords (_tweet, _keyword) 
			VALUES ('%s', (SELECT _id FROM keywords WHERE word = '%s'))"""

		self.conn.execute(delete)

		for k in keywords:
			self.conn.execute(insert % (tweet_id, k))
				
	def _get_last_twt_kword(self, default):

		query = """SELECT timestamp FROM tweets 
			WHERE _id = IFNULL((
				SELECT _tweet FROM tweet_keywords ORDER BY _id DESC LIMIT 1
			), -1)
			LIMIT 1"""
		cursor = self.conn.execute(query)
		if cursor.rowcount != 0:
			default = cursor.fetchone()[0].strftime("%Y-%m-%d %H:%M:%S")

		return default

	#NB!: Major back-end method - responsible for parsing a tweet and divide it into tables in a db
	def store_data(self, tweet): #id, usr, text, lat, lng, timestamp
		twt = tweet.dict()
		keywords = self._get_keywords(twt['text'])
		date = self._parse_timestamp(twt['time'])

		#insert keywords
		self.insert_keywords(keywords)
		#insert clouds
		self.insert_counters(keywords, twt['lat'], twt['lng'], date)
		#insert tweet_keyword relation
		self.insert_twt_kwords(keywords, twt['_id'])
