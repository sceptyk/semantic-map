from collector import Collector
from model.tweet import Tweet
import tweepy
import urllib
import MySQLdb
import json
import time

class Twitter_Collector(Collector):
	"""Collects tweets from twitter Web API"""

	def __init__(self, *args, **kwargs):
		"""Create table to store tweets if not exists"""
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
		self.db.execute(sql)

		self.last_id = '' 	#store last id fetched to no duplicate results

	def authorize(self):
		auth = tweepy.OAuthHandler(self._CLIENT_KEY, self._CLIENT_SECRET)
		auth.set_access_token(self._ACCESS_TOKEN, self._ACCESS_SECRET)
		 
		return tweepy.API(auth)

	def process_data(self, data):
		"""Process single data obtained from server"""
		tweet_id = data['id'] 						#tweet id {int}
		user = data['user']['id'] 					#user id {int}
		text = repr(data['text'].encode('utf_8'))	#encode the string and escape quotes
		lat = data['coordinates']['coordinates'][1]	#latitude of tweet {float}
		lng = data['coordinates']['coordinates'][0]	#longitude of tweet {float}
		timestamp = repr(time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(data['created_at'],"%a %b %d %H:%M:%S +0000 %Y"))) #convert to mysql timestamp

		tweet = Tweet(tweet_id, user, text, lat, lng, timestamp) #populate model object
		return tweet

	def get_data(self):
		"""Query server for data and process it
			@return {iterable}"""

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
			values = tweet.tuple()

			sql = """INSERT INTO tweets
				(_id, user, text, lat, lng, timestamp) 
				VALUES 
				('%u', '%u', %s, '%f', '%f', %s)""" % values

			self.db.execute(sql)

			self.last_id = values[0]

	def run(self):
		print("running Twitter Collector -----------")
		
		while True:
			try:
				data = self.get_data()
				self.store_data(data)
			except tweepy.error.TweepError:
				time.sleep(960.0)#timeout 16mins to avoid twitter rate policy
			else:
				break