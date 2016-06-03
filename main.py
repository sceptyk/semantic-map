#!usr/bin/python

from collector.foursquare_collector import Foursquare_Collector
from collector.osm_collector import Osm_Collector
from collector.twitter_collector import Twitter_Collector

import threading

twitter = Twitter_Collector(
	client_key = 'd7zekGyp2jiMyd65ACGsfOECy',
	client_secret = 'xqiOuIlRFE9KWnYZJ7H85yFkzePVOJhEAT1o1dIQ18LKlLppw6',
	access_token = '737973216262639618-KPr0Fvuk6AUgsm8AjCTZbBhAe3fzLN4',
	access_secret = 'TQHGxJIQ8ze01jtsy9o65FwThvIK8JdHCsFnvWkNB5yfs',
	dbuser = 'sceptyk',
	dbpass = 'sceptyk')

print("Twitter data ------")

last_id = ''
def fetch_data():
	try:
		data = twitter.get_data(last_id)
		last_id = twitter.store_data(data)
	except:
		threading.Timer(960.0, fetch_data).start() #timeout 15mins to avoid twitter rate policy

#fetch_data
