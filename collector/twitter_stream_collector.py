from twitter_collector import Twitter_Collector
import tweepy
import json
import MySQLdb
from model.tweet import Tweet
import time

class Twitter_Stream_Collector(Twitter_Collector):

	class _StreamListener(tweepy.StreamListener):

		def __init__(self, outer):
			self.outer = outer

		def on_data(self, data):
			_json = json.loads(data)

			if _json['coordinates'] is None:
				_json['coordinates'] = {'coordinates': [0.0,0.0]}
			else:
				parsed = self.outer.process_data(_json)
				self.outer.store_data([parsed])

			return True

		def on_error(self, status):
			print(status)
			return True

#################################################################


	def authorize(self):
		auth = tweepy.OAuthHandler(self._CLIENT_KEY, self._CLIENT_SECRET)
		auth.set_access_token(self._ACCESS_TOKEN, self._ACCESS_SECRET)

		stream_reader = self._StreamListener(self)
		stream = tweepy.Stream(auth, stream_reader)

		return stream

	def run(self):
		print("running Twitter Stream Collector -----------")

		self.client.filter(locations = [-6.421509, 53.189579, -6.017761, 53.447171], async=True)
		self.db.close()