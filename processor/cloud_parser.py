from model.tweet import Tweet
from collector.mysql_connect import Mysql_Connect
from util.util import Util
import time
import string
import math
import datetime
import os

class Cloud_Parser(object):
	"""Parse collected data, retrieve keywords and store them"""
	def __init__(self):
		self.conn = Mysql_Connect().get_connection()
		self.cursor = self.conn.cursor()
		self.util = Util()

	def get_data(self):
		loc_cursor = self.conn.cursor()
		chunk_size = 100
		start_time = time.strftime("2016-06-08 00:00:00")
		while True:
			sql = """SELECT * FROM tweets where timestamp > '%s' LIMIT %s"""

			try:
				loc_cursor.execute(sql, (start_time, chunk_size))
				results = loc_cursor.fetchall()
				if len(results)==0:
					time.sleep(3600)
					continue
				for row in results:
					twt = Tweet()
					twt.populate(row)
					try:
						self.store_data(twt)
					except:
						continue
				last = results[-1].dict()
				start_time = last['time']
			except Exception, e:
				print e
				time.sleep(3600) #if no new data - sleep for 1 hr

	def reset_increment(self):
		clear = """truncate table cloud"""
		self.cursor.execute(clear)

#START: word/char elimination
	def elim_useless(self, txt):
		clear_txt = self.clear_punctuation(txt)
		list = clear_txt.split(' ')
		r_list = []
		stop = self.stopwords()
		for word in list:
			if word.lower() in stop or len(word) <= 2 or self.has_numbers(word) or self.has_http(word):
				continue
			else:
				r_list.append(word)
		return r_list

	def clear_punctuation(self, text):
		clear_string = ""
		delimiters = string.punctuation + "\n\t"
		for symbol in text:
			if symbol not in delimiters:
				clear_string += symbol
		return clear_string

	def stopwords(self):
		with open(os.path.normpath("processor/stopwords.txt")) as input:
			text = input.readline()
		list = text.split(",")
		clear_list = []
		for word in list:
			clear_list.append(word.replace(" ", ""))
		return clear_list

	def has_numbers(self, word):
		return any(ch.isdigit() for ch in word)

	def has_http(self, word):
		return word.startswith('http')
	#END: word/char elimination

	#START: Keywords table
	def fetch_keyword_id(self, word):
		loc_cursor = self.conn.cursor()
		query = """select _id from keywords where word = %s"""
		loc_cursor.execute(query,word.lower())
		return loc_cursor.fetchall()[0][0]

	def insert_keyword(self, list):
		loc_cursor = self.conn.cursor()
		query = """INSERT IGNORE INTO keywords (word) VALUES (%s)"""
		keywords = []
		for i in range(len(list)):
			keywords.append(list[i].lower())
		try:
			loc_cursor.executemany(query, keywords)
			self.conn.commit()
		except:
			self.conn.rollback()

	#END: Keywords table

	#START: Counter table
	def insert_counter(self, kword, cloud, time_index, day):
		loc_cursor = self.conn.cursor()
		query = """insert into word_counter (_keyword, _cloud, time_index, day) values ('%s', '%s', '%s', %s)"""
		if self.fetch_counter_id(kword, cloud, time_index, day) is None:
			loc_cursor.execute(query, (kword, cloud, time_index, day))
			self.conn.commit()
			self.incr_counter_count(self.fetch_counter_id(kword, cloud, time_index, day), kword, cloud, time_index, day)
		else:
			self.incr_counter_count(self.fetch_counter_id(kword, cloud, time_index, day), kword, cloud, time_index, day)

	def incr_counter_count(self, id, kw,cloud, time_index, day):
		loc_cursor = self.conn.cursor()
		fetch = """update word_counter set count = count + 1 where _id = '%s' and _keyword = '%s' and _cloud = '%s' and time_index = '%s' and day = %s"""
		try:
			loc_cursor.execute(fetch, (id, kw,cloud, time_index, day))
			self.conn.commit()
		except:
			self.conn.rollback()
			raise Exception("Counter id doesnt exist")

	def fetch_counter_id(self, word, cloud, time_index, day):
		loc_cursor = self.conn.cursor()
		query = """select _id from word_counter where _keyword = '%s' and _cloud = '%s' and time_index = '%s' and day = %s"""
		try:
			loc_cursor.execute(query, (word, cloud, time_index, day))
			return loc_cursor.fetchall()[0][0]
		except:
			return None

	def fetch_counter_cloud(self, id):
		loc_cursor = self.conn.cursor()
		query = """select _cloud from word_counter where _id = '%s'"""
		try:
			loc_cursor.execute(query, id)
			return loc_cursor.fetchall()[0][0]
		except:
			raise Exception("Counter doesnt exist")

	def fetch_counter_kword(self, id):
		loc_cursor = self.conn.cursor()
		query = """select _kword from word_counter where _id = '%s'"""
		try:
			loc_cursor.execute(query, id)
			return loc_cursor.fetchall()[0][0]
		except:
			raise Exception("Counter doesnt exist")
	#END: Counter table

	# START: Tweet_keyword table
	def insert_twt_keyword(self, tweet_id, kword):
		loc_cursor = self.conn.cursor()
		update_query = """update tweet_keywords set _tweet = '%s' and _keyword = '%s' where _tweet =
							(select _id from tweets WHERE timestamp <= DATE_SUB(NOW(), INTERVAL 2 WEEK limit 1)) limit 1;"""
		insert_query = """insert into tweet_keywords (_tweet, _keyword) values ('%s', '%s');"""
		check_query = """select 1 from tweets WHERE timestamp <= DATE_SUB(NOW(), INTERVAL 2 WEEK) limit 1;"""
		try:
			loc_cursor.execute(update_query, (tweet_id, self.fetch_keyword_id(kword)))
			self.conn.commit()
		except:
			self.conn.rollback()

	def fetch_twt_kword__tweet(self, kword):
		loc_cursor = self.conn.cursor()
		query = """select _tweet from tweet_keyword where _keyword = '%s'"""
		loc_cursor.execute(query, self.fetch_keyword_id(kword))
		return loc_cursor.fetchall()[0]

	def fetch_twt_kword_kword(self, tweet_id):
		loc_cursor = self.conn.cursor()
		query = """select _tweet from tweet_keyword where _tweet = '%s'"""
		loc_cursor.execute(query, self.fetch_keyword_id(tweet_id))
		return loc_cursor.fetchall()[0]

	def fetch_twt_kword_id(self, tweet_id, kword):
		loc_cursor = self.conn.cursor()
		query = """select _id from tweet_keyword where _tweet = '%s' and _keyword = '%s' """
		loc_cursor.execute(query, (tweet_id, self.fetch_keyword_id(kword)))
		return loc_cursor.fetchall()[0]

	# END: Tweet_keyword table

	#START: Cloud

	def insert_cloud(self, hash, precision):
		loc_cursor = self.conn.cursor()
		query = """insert into cloud (cloud, layer) values ('%s', '%s');"""
		try:
			loc_cursor.execute(query, (hash, self.index_precision(precision)))
			self.conn.commit()
		except:
			self.conn.rollback()

	def index_precision(self, precision):  # For scaling
		if precision == 0.2:
			return 1
		elif precision == 0.6:
			return 2
		elif precision == 1.2:
			return 3
		else:
			return 0

	def fetch_layer(self, hash):
		loc_cursor = self.conn.cursor()
		query = """select layer from cloud where cloud = hash;"""
		loc_cursor.execute(query, hash)
		return self.cursor.fetchall()[0]

	def fetch_clouds(self, layer):
		loc_cursor = self.conn.cursor()
		query = """select cloud from cloud where layer = layer;"""
		loc_cursor.execute(query, layer)
		return loc_cursor.fetchall()

	#END: Cloud

	def parse_timestamp(self, timestamp):  # 2016-06-07
		week_day = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']
		timestamp = str(timestamp)
		wk = week_day[datetime.date(int(timestamp[0:4]), int(timestamp[5:7]), int(timestamp[8:10])).weekday() + 1]
		t = time.strftime(timestamp[11:20])
		return wk, t

	def time_index(self, t):
		if time.strftime('04:00:00') <= t <= time.strftime('11:59:59') : return 1
		if time.strftime('12:00:00') <= t <= time.strftime('16:59:59'): return 2
		if time.strftime('17:00:00') <= t <= time.strftime('21:59:59'): return 3
		if time.strftime('22:00:00') <= t <= time.strftime('03:59:59') : return 4
		else: return 0

	def store_data(self, tweet): #id, usr, text, lat, lng, timestamp
		tweet = tweet.dict()
		tweet_id = tweet['_id']
		text = self.elim_useless(tweet['text'])
		day = self.parse_timestamp(tweet['time'])
		time_i = self.time_index(day[1])
		lyrs = [0.2,0.6,1.2] #scaling: 0.2 = 0.2*1km
		cloud = [] #must be 3 hashed values

		self.insert_keyword(self.elim_useless(tweet['text']))

		for each in lyrs:
			cloud.append(self.util.hash_geo(tweet['lat'],tweet['lng'],each))
			self.insert_cloud(hash, self.index_precision(each))

		for each in self.elim_useless(tweet['text']):
			for c in cloud:
				self.insert_counter(self.fetch_keyword_id(each),c ,time_i, day[0])

		for word in text: self.insert_twt_keyword(tweet_id, word)
