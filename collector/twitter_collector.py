from collector import Collector
from tweet import Tweet
import tweepy
import urllib
import MySQLdb
import time

class Twitter_Collector(Collector):

	def __init__(self):
		super(Collector, self).__init__()
		self.cursor.execute("""CREATE TABLE tweets (
			_id PRIMARY KEY UNSIGNED BIGINT,
			lat DOUBLE(12,7),
			lng DOUBLE(12,7),
			user UNSIGNED BIGINT,
			text CHAR(255),
			timestamp TIMESTAMP
		)""")

	def authorize(self):
		auth = tweepy.OAuthHandler(self._CLIENT_KEY, self._CLIENT_SECRET)
		auth.set_access_token(self._ACCESS_TOKEN, self._ACCESS_SECRET)
		 
		return tweepy.API(auth)

	def process_data(self, data):
		tweet_id = data['id']
		lat = data['coordinates']['coordinates'][1]
		lng = data['coordinates']['coordinates'][0]
		user = data['user']['id']
		timestamp = time.strftime(time.strptime(data['created_at'],"%a %b %d %H:%M:%S +0000 %Y"), '%Y-%m-%d %H:%M:%S') #convert to mysql timestamp
		text = data['text']

		yield Tweet(tweet_id, lat, lng, user, timestamp, text)

	def get_data(self, max_id = ''):
		for tweet in tweepy.Cursor(self.client.search, 
										q = urllib.quote_plus(''),
										geocode = '53.3447512,-6.2686897,20km', #centre of Dublin
										result_type = 'recent',
										max_id = max_id, #search for entries before the id
										count = 100
									).items():
			yield self.process_data(tweet._json)
			
	def store_data(self, data):
		print('Store data')
		sql = """INSERT INTO 
			tweets(_id, lat, lng, user, text, timestamp) 
			VALUES (%u, %f, %f, %u, %s, %s)""" % data._touple

		try:
			self.cursor.execute(sql)
			self.conn.commit()
		except:
			self.conn.rollback()