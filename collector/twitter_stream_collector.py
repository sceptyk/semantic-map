from twitter_collector import Twitter_Collector
import tweepy
import json
import MySQLdb
from tweet import Tweet
import time

class Twitter_Stream_Collector(Twitter_Collector):

	class _StreamListener(tweepy.StreamListener):

		def __init__(self, outer):
			self.outer = outer

		def on_data(self, data):
			_json = json.loads(data)
			parsed = self.outer.process_data(_json)
			self.outer.store_data()

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
		print("running -----------")

		while True:
			try:
				self.client.filter(locations = [-6.421509, 53.189579, -6.017761, 53.447171])
				#time.sleep(960.0)#timeout 16mins to avoid twitter rate policy
			except:
				break
				
		self.conn.close()