from model.tweet import Tweet
from collector.mysql_connect import Mysql_Connect
import time
import string
import math


class Cloud_Parser(object):
	"""Parse collected data, retrieve keywords and store them"""
	size_w, size_h = 64, 64
	def __init__(self):
		self.conn = Mysql_Connect().get_conn()
		self.cursor = self.conn.cursor()
		self.Matrix = self.get_grid()
		loc_cursor = self.conn.cursor()
		CREATE_KEYWORDS_TABLE = """CREATE TABLE keywords (
				              _id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
				              word CHAR(100) NOT NULL COMMENT '',
				              PRIMARY KEY (_id)  COMMENT '',
				              UNIQUE INDEX word_UNIQUE (word ASC)  COMMENT '');"""
		try:
			loc_cursor.execute(CREATE_KEYWORDS_TABLE)
		except:
			self.conn.rollback()
		CREATE_POINT_TABLE = """CREATE TABLE IF NOT EXISTS tweet_location (
				            _id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
				            lat DOUBLE(12, 7),
				            lng DOUBLE(12, 7),
				            _keyword BIGINT,
				            PRIMARY KEY ( _id )
				        )"""
		try:
			loc_cursor.execute(CREATE_POINT_TABLE)
		except:
			self.conn.rollback()
		CREATE_CLOUD_TABLE = """CREATE TABLE  cloud  (
				               _id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
				               start_lat  DOUBLE(12,7) NULL COMMENT '',
				               start_lng  DOUBLE(12,7) NULL COMMENT '',
				               end_lat  DOUBLE(12,7) NULL COMMENT '',
				               end_lng  DOUBLE(12,7) NULL COMMENT '',
				               start_time  TIME NULL COMMENT '',
				               end_time  TIME NULL COMMENT '',
				              PRIMARY KEY ( _id )  COMMENT '');"""
		try:
			loc_cursor.execute(CREATE_CLOUD_TABLE)
		except:
			self.conn.rollback()

		CREATE_COUNTER_TABLE = """CREATE TABLE  word_counter  (
							   _id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
							   _keyword  BIGINT UNSIGNED NOT NULL COMMENT '',
							   _cloud  BIGINT UNSIGNED NOT NULL COMMENT '',
							   count  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '',
							  PRIMARY KEY ( _id )  COMMENT '',
							  INDEX  keywrod_idx  ( _keyword  ASC)  COMMENT '',
							  INDEX  cloud_idx  ( _cloud  ASC)  COMMENT '',
							  CONSTRAINT  keyword
								FOREIGN KEY ( _keyword )
								REFERENCES  keywords  ( _id )
								ON DELETE NO ACTION
								ON UPDATE NO ACTION,
							  CONSTRAINT  cloud
								FOREIGN KEY (_cloud)
								REFERENCES cloud  ( _id )
								ON DELETE NO ACTION
								ON UPDATE NO ACTION);"""
		try:
			loc_cursor.execute(CREATE_COUNTER_TABLE)
		except:
			self.conn.rollback()

		CREATE_TWEET_KEYWORDS_TABLE = """CREATE TABLE tweet_keywords  (
				               _id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
				               _tweet  BIGINT UNSIGNED NOT NULL COMMENT '',
				               _keyword  BIGINT UNSIGNED NOT NULL COMMENT '',
				              PRIMARY KEY ( _id )  COMMENT '',
				              INDEX  tweet_idx  ( _tweet  ASC)  COMMENT '',
				              INDEX  keyword_idx  ( _keyword  ASC)  COMMENT '',
				              CONSTRAINT  tweet
				                FOREIGN KEY ( _tweet )
				                REFERENCES  tweets  ( _id )
				                ON DELETE NO ACTION
				                ON UPDATE NO ACTION,
				              CONSTRAINT  keyword_tweet
				                FOREIGN KEY ( _keyword )
				                REFERENCES keywords  ( _id )
				                ON DELETE NO ACTION
				                ON UPDATE NO ACTION);"""

		try:
			loc_cursor.execute(CREATE_TWEET_KEYWORDS_TABLE)
		except:
			self.conn.rollback()
	
	def process(self, data):
		emoticons_str = r"""
		    (?:
		        [:=;] # Eyes
		        [oO\-]? # Nose (optional)
		        [D\)\]\(\]/\\OpP] # Mouth
		    )"""
		 
		regex_str = [
		    emoticons_str,
		    r'<[^>]+>', # HTML tags
		    r'(?:@[\w_]+)', # @-mentions
		    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
		    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
		 
		    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
		    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
		    r'(?:[\w_]+)', # other words
		    r'(?:\S)' # anything else
		]
		    
		tokens_re = regex_str.compile(r'('+'|'.join(regex_str)+')', regex_str.VERBOSE | regex_str.IGNORECASE)
		emoticon_re = emoticons_str.compile(r'^'+emoticons_str+'$', emoticons_str.VERBOSE | emoticons_str.IGNORECASE)
		 
		def tokenize(s):
			return tokens_re.findall(s)
		 
		def preprocess(s, lowercase=False):
			tokens = tokenize(s)
			if lowercase:
				tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
			return tokens
		return preprocess(data)

	def get_data(self):
		#TODO connect to db
		
		chunk_size = 1000

		while True:
			start = 0
			end = chunk_size
			sql = """SELECT * FROM tweets ORDER BY _id LIMIT %d, %d""" % (start, end)

			try:
				self.cursor.execute(sql)
				results = self.cursor.fetchall()
				for row in results:
					tweet = Tweet()
					tweet.populate(row)

					self.store_data(tweet)

				start = end
				end += chunk_size
			except:
				print "Error: unable to fecth data"
				break

	def get_grid(self):
		loc_rix = []
		start_lat = 53.39806981341725
		for i in range(self.size_h):
			loc_rix.append([])
			start_lng = -6.3686370849609375
			for j in range(self.size_w):
				loc_rix[i].append((start_lat, start_lng))
				start_lng += 0.00488173872
			start_lat -= 0.003145015
		return loc_rix

	def elim_useless(self, txt):
		list = txt.split(' ')
		r_list = []
		for word in list:
			if word.lower() in self.stopwords() or len(word) <= 2:
				continue
			else:
				r_list.append(word)
		return r_list

	def populate_clouds(self):
		#self.reset_increment()
		for i in range(self.size_h - 1):
			for j in range(self.size_w - 1):
				# Morning tweets
				pop_q = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
									VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (
					self.Matrix[i][j][0], self.Matrix[i][j][1], self.Matrix[i + 1][j + 1][0],
					self.Matrix[i + 1][j + 1][1], time.strftime('4:00:00'), time.strftime('11:59:59'))
				try:
					self.cursor.execute(pop_q)
					self.conn.commit()
				except:
					self.conn.rollback()
				# Afternoon tweets
				pop_q = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
									VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (
					self.Matrix[i][j][0], self.Matrix[i][j][1], self.Matrix[i + 1][j + 1][0],
					self.Matrix[i + 1][j + 1][1], time.strftime('12:00:00'), time.strftime('16:59:59'))
				try:
					self.cursor.execute(pop_q)
					self.conn.commit()
				except:
					self.conn.rollback()
				# Evening tweets
				pop_q = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
									VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (
					self.Matrix[i][j][0], self.Matrix[i][j][1], self.Matrix[i + 1][j + 1][0],
					self.Matrix[i + 1][j + 1][1], time.strftime('17:00:00'), time.strftime('21:59:59'))
				try:
					self.cursor.execute(pop_q)
					self.conn.commit()
				except:
					self.conn.rollback()
				# Night time tweets
				pop_q = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
									VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (
					self.Matrix[i][j][0], self.Matrix[i][j][1], self.Matrix[i + 1][j + 1][0],
					self.Matrix[i + 1][j + 1][1], time.strftime('22:00:00'), time.strftime('3:59:59'))
				try:
					self.cursor.execute(pop_q)
					self.conn.commit()
				except:
					self.conn.rollback()

	def reset_increment(self):
		clear = """truncate table cloud"""
		self.cursor.execute(clear)

	def get_clouds_count(self):
		count_query = """select count(*) from cloud;"""
		loc_cursor = self.conn.cursor()
		loc_cursor.execute(count_query)
		return loc_cursor.fetchall()

	def point_in_cloud(self, p_lat, p_lng):
		EDGE = 0.00000000001
		loc_cursor = self.conn.cursor()
		# q = """select start_lat from cloud where _id=%d;"""
		for id in range(1, self.get_clouds_count()):
			start_latQ = """select start_lat from cloud where _id=%d;""" % id
			loc_cursor.execute(start_latQ)
			start_lat = "%20.15lf" % loc_cursor.fetchone()[0]

			start_lngQ = """select start_lng from cloud where _id=%d;""" % id
			loc_cursor.execute(start_lngQ)
			start_lng = "%20.15lf" % loc_cursor.fetchone()[0]

			end_latQ = """select end_lat from cloud where _id=%d;""" % id
			loc_cursor.execute(end_latQ)
			end_lat = "%20.15lf" % loc_cursor.fetchone()[0]

			end_lngQ = """select end_lng from cloud where _id=%d;""" % id
			loc_cursor.execute(end_lngQ)
			end_lng = "%20.15lf" % loc_cursor.fetchone()[0]
			if start_lng == p_lng:
				p_lng += EDGE
			if start_lat == p_lat:
				p_lat += EDGE
			if end_lng == p_lng:
				p_lng -= EDGE
			if end_lat == p_lat:
				p_lat -= EDGE
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
		return "Not found"

	def get_cloud_coords(self, id):
		local_cursor = self.conn.cursor()
		query = "select * from cloud where _id=%d" % id
		local_cursor.execute(query)
		fetch = local_cursor.fetchall()[0]
		return [fetch[1], fetch[2], fetch[3], fetch[4]]

	def glob_cloud(self, tweet):
		parse_tweet = tweet.dict()
		tweet_text = parse_tweet['text']
		global_text_data = self.elim_useless(tweet_text)
		clear_global_text_data = self.clear_punctuation(global_text_data)
		for text in clear_global_text_data:
			for word in text:
				feed = """INSERT IGNORE INTO keywords (word) VALUES (%s)""" % word
				try:
					self.cursor.execute(feed)
					self.conn.commit()
				except:
					self.conn.rollback()

	def clear_punctuation(self, text):
		clear_string = ""
		delimiters = string.punctuation + "\n\t"
		for symbol in text:
			if symbol not in delimiters:
				clear_string += symbol
		return clear_string


	def store_data(self, data):
		pass
		#TODO update each branch
		# -- each place based on location
		# -- each time part
		# -- global list (done)
		# 
		
		# update cloud table
		# add locations clouds
		
		# add time clouds
		#START::global word cloud
		#take text out

		#END::global word cloud

		#START::location based wordclouds
		#Dublin divided into 250m*250m squares in total of 14840 squares
		#END::location based wordclouds
		#update keywords table


		#update cloud_count table
	def stopwords(self):
		with open("stopwords.txt") as input:
			text = input.readline()
		list = text.split(",")
		clear_list = []
		for word in list:
			clear_list.append(word.replace(" ", ""))
		return clear_list

	def get_cloud_id(self, s_lat, s_lng, end_lat, end_lng):
		loc_cursor = self.conn.cursor()
		output = []
		query = """select _id from cloud where start_lat = %20.15lf AND start_lng = %20.15lf and end_lat = %20.15lf
																and end_lng = %20.15lf""" % (s_lat, s_lng,
																							 end_lat, end_lng)
		loc_cursor.execute(query)
		out = loc_cursor.fetchall()
		for i in range(0, 4):
			output.append(out[i][0])
		return output

	def coord_in_matrix(self, lat, lng):
		for i in range(self.size_h):
			for j in range(self.size_w):
				if self.Matrix[i][j] == (float(lat), float(lng)):
					return True
		return False

	def cloud_exists(self, s_lat, s_lng, e_lat, e_lng):
		loc_cursor = self.conn.cursor()
		query = """ select exists(select * from cloud where start_lat = %20.15lf and start_lng = %20.15lf and end_lat = %20.15lf
																and end_lng = %20.15lf);""" % (s_lat, s_lng,
																							   e_lat, e_lng)
		loc_cursor.execute(query)
		return loc_cursor.fetchall()[0][0]

	def custom_cloud(self, start_lat, start_lng, end_lat,
					 end_lng):  # 10000% private method - user MUST NOT be able to access it under any circumstances
		loc_cursor = self.conn.cursor()
		if self.coord_in_matrix(start_lat, start_lng):
			if self.coord_in_matrix(end_lat,
									end_lng):  # Some calculations will be needed in order to identify the actual position
				if float(start_lat) > float(end_lat):
					if float(start_lng) < float(end_lng):
						if self.cloud_exists(start_lat, start_lng, end_lat, end_lng) != 1:
							# All checks passed - can make a safe query now
							morning = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
								VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (start_lat, start_lng,
																								  end_lat, end_lng,
																								  time.strftime(
																									  '4:00:00'),
																								  time.strftime(
																									  '11:59:59'))
							try:
								loc_cursor.execute(morning)
								self.conn.commit()
							except:
								self.conn.rollback()
							a_noon = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
								VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (start_lat, start_lng,
																								  end_lat, end_lng,
																								  time.strftime(
																									  '12:00:00'),
																								  time.strftime(
																									  '16:59:59'))
							try:
								loc_cursor.execute(a_noon)
								self.conn.commit()
							except:
								self.conn.rollback()
							evenin = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
								VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (start_lat, start_lng,
																								  end_lat, end_lng,
																								  time.strftime(
																									  '17:00:00'),
																								  time.strftime(
																									  '21:59:59'))
							try:
								loc_cursor.execute(evenin)
								self.conn.commit()
							except:
								self.conn.rollback()
							night = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
								VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (start_lat, start_lng,
																								  end_lat, end_lng,
																								  time.strftime(
																									  '22:00:00'),
																								  time.strftime(
																									  '3:59:59'))
							try:
								loc_cursor.execute(night)
								self.conn.commit()
							except:
								self.conn.rollback()
		return_id = """select _id where start_lat = %20.15lf and start_lng = %20.15lf and end_lat = %20.15lf
																and end_lng = %20.15lf;""" % (start_lat, start_lng,
																							  end_lat, end_lng)
		return self.get_cloud_id(start_lat, start_lng, end_lat, end_lng)

	def clouds_in_clouds(self, cloud_id):
		coords = self.get_cloud_coords(cloud_id)  # [0] start_lat, [1] start_lng, [2] end_lat, [3] end_lng
		loc_cursor = self.conn.cursor()
		result = []
		query = """select _id from cloud where
						start_lat >  %20.15lf and
						start_lat <= %20.15lf and
						start_lng <  %20.15lf and
						start_lng >= %20.15lf and
						end_lat   <  %20.15lf and
						end_lat	  >= %20.15lf and
						end_lng   >  %20.15lf and
						end_lng   <= %20.15lf;
						""" % (coords[2], coords[0], coords[3], coords[1], coords[0], coords[2], coords[1], coords[3])
		loc_cursor.execute(query)
		out = loc_cursor.fetchall()
		for i in range(0, len(out)):
			result.append(out[i][0])
		return result

	def fetch_keyword_id(self, word):
		loc_cursor = self.conn.cursor()
		query = """select _id from keywords where word = ('%s')""" % word
		loc_cursor.execute(query)
		return loc_cursor.fetchall()[0][0]

	def insert_keyword(self, list):
		loc_cursor = self.conn.cursor()
		for i in range(len(list)):
			query = """INSERT IGNORE INTO keywords (word) VALUES ('%s')""" % list[i]
			if self.kword_exist(list[i]) != 1:
				print self.kword_exist(list[i])
				try:
					loc_cursor.execute(query)
					self.conn.commit()
				except:
					self.conn.rollback()
			else:
				continue

	def kword_exist(self, word):
		loc_cursor = self.conn.cursor()
		query = """select 1 from keywords where word = ('%s')""" % word
		loc_cursor.execute(query)
		try:
			return loc_cursor.fetchall()[0][0]
		except:
			return 0

	# START: Helper functions to calculate metres per 1 degree considering the Earth's elevation
	def rlat(self, deg):
		return (deg * math.pi) / 180

	def metres_per_lat(self, rlat):
		return 111132.92 - 559.82 * math.cos(2 * rlat)

	def rlng(self, deg):
		return (50 * math.pi) / 180

	def metres_per_lng(self, rlng):
		return 111412.84 * math.cos(rlng) - 93.5 * math.cos(3 * rlng)
		# END: Helper functions to calculate metres per 1 degree considering the Earth's elevation

	def insert_layer(self, layer, s_time, e_time, day):  # 5 layers - 0 to 4 (timestamp - time.strftime('22:00:00'))
		loc_cursor = self.conn.cursor()
		itr = int(math.pow(2, layer) - 1)
		for i in range(0, self.size_h - itr, itr):
			for j in range(0, self.size_w - itr, itr):
				query = """insert into cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time, layer, day)
										values (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s', %d, '%s')""" % (
					self.Matrix[i][j][0], self.Matrix[i][j][1],
					self.Matrix[i + itr][j + itr][0], self.Matrix[i + itr][j + itr][1], s_time, e_time, layer, day)
				try:
					loc_cursor.execute(query)
					self.conn.commit()
				except:
					self.conn.rollback()