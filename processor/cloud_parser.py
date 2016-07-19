from model.tweet import Tweet
from collector.mysql_connect import Mysql_Connect
import time
import string
import math
import datetime
import os
import Geohash as geo


class Cloud_Parser(object):
	"""Parse collected data, retrieve keywords and store them"""
	size_w, size_h = 64, 64
	EDGE = 0.00000000001
	def __init__(self):
		self.conn = Mysql_Connect().get_connection()
		self.cursor = self.conn.cursor()

	def get_data(self):
		loc_cursor = self.conn.cursor()
		chunk_size = 10
		start = 0
		end = chunk_size
		while True:
			sql = """SELECT * FROM tweets ORDER BY _id LIMIT %s, %s"""

			try:
				loc_cursor.execute(sql, (start, end))
				results = loc_cursor.fetchall()
				for row in results:
					twt = Tweet()
					twt.populate(row)
					try:
						self.store_data(twt)
					except:
						continue
				start = end
				end += chunk_size
			except Exception, e:
				tweet = twt.dict()
				print tweet['_id']
				print str(e)

	def get_grid(self):
		loc_rix = []
		start_lat = 53.39806981341725
		for i in range(0, self.size_h):
			loc_rix.append([])
			start_lng = -6.3686370849609375
			for j in range(0, self.size_w):
				loc_rix[i].append((start_lat, start_lng))
				start_lng += 0.00488173872
			start_lat -= 0.003145015
		return loc_rix

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

	#START: Helper functions to calculate metres per 1 degree considering the Earth's elevation
	def rlat(self, deg):
		return (deg * math.pi) / 180

	def metres_per_lat(self, rlat):
		return 111132.92 - 559.82 * math.cos(2 * rlat)

	def rlng(self, deg):
		return (deg * math.pi) / 180

	def metres_per_lng(self, rlng):
		return 111412.84 * math.cos(rlng) - 93.5 * math.cos(3 * rlng)
	#END: Helper functions to calculate metres per 1 degree considering the Earth's elevation

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

	#START: Cloud Table

	def point_in_cloud(self, p_lat, p_lng, layer):
		fetch = self.help_point_in_cloud(layer)

		for coords in range(0, len(fetch)):
			id = fetch[coords][0]
			decode_start = geo.decode(fetch[coords][1])
			decode_end = geo.decode(fetch[coords][2])

			start_lat = "%20.15lf" % float(decode_start[0])
			start_lng = "%20.15lf" % float(decode_start[1])
			end_lat = "%20.15lf" % float(decode_end[0])
			end_lng = "%20.15lf" % float(decode_end[1])

			if start_lng == p_lng:
				p_lng += self.EDGE
			if start_lat == p_lat:
				p_lat += self.EDGE
			if end_lng == p_lng:
				p_lng -= self.EDGE
			if end_lat == p_lat:
				p_lat -= self.EDGE
			if float(start_lng) < float(p_lng):
				if float(start_lat) > float(p_lat):
					if float(end_lng) > float(p_lng):
						if float(end_lat) < float(p_lat):
							return id
						else:
							continue
					else:
						continue
				else:
					continue
			else:
				continue
		return 0

	def help_point_in_cloud(self, layer):
		loc_cursor = self.conn.cursor()
		list = []
		query = """select _id, start_crds, end_crds from cloud where layer = '%s' """ % layer
		loc_cursor.execute(query)
		for each in loc_cursor.fetchall():
			list.append(each)
		return list

	def get_cloud_coords(self, id):
		local_cursor = self.conn.cursor()
		query = """select start_crds, end_crds from cloud where _id = '%s'""" % id
		local_cursor.execute(query)
		return local_cursor.fetchall()[0]

	def cloud_exists(self, start_crds, end_crds):
		loc_cursor = self.conn.cursor()
		query = """ select exists(select * from cloud where start_crds = %s and end_crds = %s);"""
		loc_cursor.execute(query, (start_crds, end_crds))
		return loc_cursor.fetchall()[0][0]

	def get_cloud_id(self, start_crds, end_crds, layer):
		loc_cursor = self.conn.cursor()
		output = []
		query = """select _id from cloud where start_crds = %s AND end_crds = %s and layer = '%s'"""
		try:
			loc_cursor.execute(query, (start_crds, end_crds, layer))
		except:
			raise Exception("Cant fetch id - cloud doesnt exist")
		out = loc_cursor.fetchall()
		for i in range(0, 4):
			output.append(out[i][0])
		return output

	def layers(self, layer):
		loc_cursor = self.conn.cursor()
		query = """select start_crds, end_crds from cloud where layer = '%s' """
		loc_cursor.execute(query, layer)
		return loc_cursor.fetchall()

	def get_layer_coords(self, layer):
		loc_cursor = self.conn.cursor()
		fetch = []
		if 0 <= layer < 5:
			query = """select start_crds, end_crds from cloud where layer = '%s'"""
			loc_cursor.execute(query, layer)
			fetch = loc_cursor.fetchall()
		return fetch

	#END: Cloud Table

	# START: Tweet_keyword table
	def insert_twt_keyword(self, tweet_id, kword):
		loc_cursor = self.conn.cursor()
		query = """insert into tweet_keywords (_tweet, _keyword) values ('%s', '%s')"""
		try:
			loc_cursor.execute(query, (tweet_id, self.fetch_keyword_id(kword)))
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

	def store_data(self, tweet):
		tweet = tweet.dict()
		text = self.elim_useless(tweet['text'])
		day = self.parse_timestamp(tweet['time'])
		time_i = self.time_index(day[1])
		lyrs = [0,2,4]
		tweet_id = tweet['_id']
		cloud = []
		self.insert_keyword(text)

		for layer in lyrs:
			cloud.append(self.point_in_cloud(tweet['lat'], tweet['lng'], layer))
		if cloud[0] == 0: raise LookupError("Cloud not found")
		for each in text:
			for c in cloud:
				self.insert_counter(self.fetch_keyword_id(each),c ,time_i, day[0])
		for word in text: self.insert_twt_keyword(tweet_id, word)

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

