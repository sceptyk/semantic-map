from collector import Collector
from node import Node
import tweepy
import urllib
import json

class Twitter_Collector(Collector):

	def authorize(self):
		auth = tweepy.OAuthHandler(self._CLIENT_KEY, self._CLIENT_SECRET)
		auth.set_access_token(self._ACCESS_TOKEN, self._ACCESS_SECRET)
		 
		return tweepy.API(auth)

	def get_data(self):
		for tweet in tweepy.Cursor(self.client.search, 
										q = urllib.quote_plus(''),
										geocode = '53.3447512,-6.2686897,20km',
										result_type = 'recent',
										#max_id = '',
										count = 100
									
									).items():
			_json = tweet._json
			yield Node(_json['id'], _json['coordinates'], _json['user']['location'], _json['created_at'], _json['text'])
			