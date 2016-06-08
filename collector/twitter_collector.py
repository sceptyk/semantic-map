from collector import Collector
from model.tweet import Tweet
import tweepy
import urllib
import MySQLdb
import json
import time

class Twitter_Collector(Collector):

	def __init__(self, *args, **kwargs):
		super(Twitter_Collector, self).__init__(*args, **kwargs)
		sql = """CREATE TABLE IF NOT EXISTS tweets (
			_id BIGINT UNSIGNED NOT NULL,
			user BIGINT UNSIGNED,
			text CHAR(255),
			lat DOUBLE(12,7),
			lng DOUBLE(12,7),
			timestamp TIMESTAMP,
			PRIMARY KEY ( _id )
		)"""
		self.cursor.execute(sql)

		self.last_id = ''

	def authorize(self):
		auth = tweepy.OAuthHandler(self._CLIENT_KEY, self._CLIENT_SECRET)
		auth.set_access_token(self._ACCESS_TOKEN, self._ACCESS_SECRET)
		 
		return tweepy.API(auth)

	def process_data(self, data):
		tweet_id = data['id']
		lat = data['coordinates']['coordinates'][1]
		lng = data['coordinates']['coordinates'][0]
		user = data['user']['id']
		text = data['text']
		timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(data['created_at'],"%a %b %d %H:%M:%S +0000 %Y")) #convert to mysql timestamp

		tweet = Tweet(tweet_id, lat, lng, user, text, timestamp)
		return tweet

	def get_data(self):
		max_id = self.last_id
		for tweet in tweepy.Cursor(self.client.search, 
										q = urllib.quote_plus(''),
										geocode = '53.3447512,-6.2686897,20km', #centre of Dublin
										result_type = 'recent',
										max_id = max_id, #search for entries before the id
										count = 100
									).items():
			
			_json = tweet._json
			if _json['coordinates'] is None:
				continue
			else:
				data = self.process_data(_json)
				yield data
			
	def store_data(self, data):
		
		for tweet in data:
			values = tweet._tuple()

			sql = """INSERT INTO tweets
				(_id, lat, lng, user, text, timestamp) 
				VALUES 
				('%f', '%f', '%s', '%f', '%f', '%s')""" % values

			try:
				self.cursor.execute(sql)
				self.conn.commit()
			except:
				self.conn.rollback()

			self.last_id = values[0]

	def run(self):
		print("running -----------")
		
		while True:
			try:
				data = self.get_data()
				self.store_data(data)
			except tweepy.error.TweepError:
				time.sleep(960.0)#timeout 16mins to avoid twitter rate policy
			else:
				break

		self.conn.close()