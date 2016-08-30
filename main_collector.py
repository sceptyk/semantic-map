#!usr/bin/env python

from collector.twitter_collector import Twitter_Collector
from collector.twitter_stream_collector import Twitter_Stream_Collector

from threading import Thread
import time
import tweepy

twitter_stream = Twitter_Stream_Collector(
	client_key = 'CLIENT_KEY',
	client_secret = 'CLIENT_SECRET',
	access_token = 'ACCESS_TOKEN',
	access_secret = 'ACCESS_SECRET')

print("Twitter data ------")

twitter_stream.run()