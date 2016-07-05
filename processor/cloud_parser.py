from model.tweet import Tweet
from collector.mysql_connect import Mysql_Connect
import time
import string
import math


class Cloud_Parser(object):
	"""Parse collected data, retrieve keywords and store them"""
	size_w, size_h = 64, 64
	EDGE = 0.00000000001
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
		               layer INT(1) NULL COMMENT '',
		               day CHAR(3) NULL COMMENT '',
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
			sql = """SELECT * FROM tweets ORDER BY _id LIMIT '%s', '%s'"""

			try:
				self.cursor.execute(sql, (start, end))
				results = self.cursor.fetchall()
				for row in results:
					tweet = Tweet()
					tweet.populate(row)

					self.store_data(tweet)

				start = end
				end += chunk_size
			except:
				raise Exception("Error: unable to fetch data")

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

	def test_grid(self):
		for i in range(self.size_h):
			for j in range(self.size_w):
				print(self.Matrix[i][j])
		print("Test separate coords")
		print(self.Matrix[0][0])
		print(self.Matrix[0][0][1])

	def reset_increment(self):
		clear = """truncate table cloud"""
		self.cursor.execute(clear)

#START: word/char elimination
	def elim_useless(self, txt):
		clear_txt = self.clear_punctuation(txt)
		list = clear_txt.split(' ')
		r_list = []
		for word in list:
			if word.lower() in self.stopwords() or len(word) <= 2:
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
		with open("stopwords.txt") as input:
			text = input.readline()
		list = text.split(",")
		clear_list = []
		for word in list:
			clear_list.append(word.replace(" ", ""))
		return clear_list
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
		for i in range(len(list)):
			query = """INSERT IGNORE INTO keywords (word) VALUES (%s)"""
			if self.kword_exist(list[i]) != 1:
				try:
					loc_cursor.execute(query, list[i].lower())
					self.conn.commit()
				except:
					self.conn.rollback()
			else:
				continue

	def kword_exist(self, word):
		loc_cursor = self.conn.cursor()
		query = """select 1 from keywords where word = %s"""
		loc_cursor.execute(query, word)
		try:
			return loc_cursor.fetchall()[0][0]
		except:
			return 0
	#END: Keywords table

	#START: Counter table
	def insert_counter(self, kword, cloud):
		loc_cursor = self.conn.cursor()
		query = """insert into word_counter (_keyword, _cloud) values ('%s', '%s')"""
		if self.fetch_counter_id is None:
			loc_cursor.execute(query, (kword, cloud))
			self.incr_counter_count(self.fetch_counter_id(kword, cloud))
		else:
			self.incr_counter_count(self.fetch_counter_id(kword, cloud))

	def incr_counter_count(self, id):
		loc_cursor = self.conn.cursor()
		fetch = """update counter set count = count + 1 where _id = '%s'"""
		try:
			loc_cursor.execute(fetch, id)
			self.conn.commit()
		except:
			self.conn.rollback()
			raise Exception("Counter id doesnt exist")

	def fetch_counter_id(self, word, cloud):
		loc_cursor = self.conn.cursor()
		query = """select _id from counter where word = '%s' and cloud = '%s'"""
		try:
			loc_cursor.execute(query, (word, cloud))
			return loc_cursor.fetchall()[0][0]
		except:
			raise Exception("Counter doesnt exist")

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

	#START: Location table
	def insert_location(self, lat, lng, kword):
		loc_cursor = self.conn.cursor()
		query = """insert into tweet_location (lat, lng, _keyword) values ('%s', '%s', '%s')"""
		try:
			loc_cursor.execute(query, (lat, lng, self.fetch_keyword_id(kword)))
			self.conn.commit()
		except:
			self.conn.rollback()

	def fetch_location_id(self, word):
		loc_cursor = self.conn.cursor()
		query = """select _id from tweet_location where _keyword = '%s'"""
		try:
			loc_cursor.execute(query, self.fetch_keyword_id(word))
			return loc_cursor.fetchall()[0]
		except:
			raise Exception("A word has no location")

	def fetch_location_coords(self, kword):
		loc_cursor = self.conn.cursor()
		query = """select lat, lng from tweet_location where _keyword = '%s'"""
		loc_cursor.execute(query, self.fetch_keyword_id(kword))
		return loc_cursor.fetchall()[0]
	#END: Location table

	#START: Cloud Table
	def populate_clouds(self):
		days = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']
		for j in range(0, 5):
			for i in days:
				self.insert_layer(j, time.strftime('4:00:00'), time.strftime('11:59:59'), i)
				self.insert_layer(j, time.strftime('12:00:00'), time.strftime('16:59:59'), i)
				self.insert_layer(j, time.strftime('17:00:00'), time.strftime('21:59:59'), i)
				self.insert_layer(j, time.strftime('22:00:00'), time.strftime('3:59:59'), i)

	def point_in_cloud(self, p_lat, p_lng, day, t, layer):
		def_day = "TUE"
		def_time = time.strftime("12:00:00")
		loc_cursor = self.conn.cursor()
		list = self.help_point_in_cloud(t, day, layer)
		# q = """select start_lat from cloud where _id=%d;"""
		for id in list:
			start_latQ = """select start_lat, start_lng, end_lat, end_lng from cloud where _id = '%s' and start_time = %s and day = %s;"""
			loc_cursor.execute(start_latQ, (id, def_time, def_day))
			fetch = loc_cursor.fetchall()
			start_lat = "%20.15lf" % fetch[0][0]
			start_lng = "%20.15lf" % fetch[0][1]
			end_lat = "%20.15lf" % fetch[0][2]
			end_lng = "%20.15lf" % fetch[0][3]

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
		return "Not found"

	def help_point_in_cloud(self, t, day, layer):
		loc_cursor = self.conn.cursor()
		list = []
		query = """select _id from cloud where start_time <= '%s' and end_time >= '%s' and day = '%s' and layer = '%s' """ % (
		t, t, day, layer)
		loc_cursor.execute(query)
		for each in loc_cursor.fetchall():
			list.append(each[0])
		return list

	def get_cloud_coords(self, id):
		local_cursor = self.conn.cursor()
		query = """select start_lat, start_lng, end_lat, end_lng from cloud where _id = '%s'""" % id
		local_cursor.execute(query)
		return local_cursor.fetchall()[0]

	def cloud_exists(self, s_lat, s_lng, e_lat, e_lng):
		loc_cursor = self.conn.cursor()
		query = """ select exists(select * from cloud where start_lat = '%s' and start_lng = '%s' and end_lat = '%s'
																	and end_lng = '%s');"""
		loc_cursor.execute(query, (s_lat, s_lng, e_lat, e_lng))
		return loc_cursor.fetchall()[0][0]

	def get_cloud_id(self, s_lat, s_lng, end_lat, end_lng, time, day, layer):
		loc_cursor = self.conn.cursor()
		output = []
		query = """select _id from cloud where start_lat = '%s' AND start_lng = '%s' and end_lat = '%s'
																		and end_lng = '%s' and time = %s and day = %s and layer = '%s'"""
		try:
			loc_cursor.execute(query, (s_lat, s_lng, end_lat, end_lng, time, day, layer))
		except:
			raise Exception("Cant fetch id - cloud doesnt exist")
		out = loc_cursor.fetchall()
		for i in range(0, 4):
			output.append(out[i][0])
		return output

	def layers(self, layer):
		loc_cursor = self.conn.cursor()
		def_time = time.strftime("12:00:00")
		def_day = "TUE"
		query = """select start_lat, start_lng, end_lat, end_lng from cloud where start_time = %s and day = %s and layer = '%s' """
		loc_cursor.execute(query, (def_time, def_day, layer))
		return loc_cursor.fetchall()

	def clouds_in_clouds(self, cloud_id):
		coords = self.get_cloud_coords(cloud_id) #[0] start_lat, [1] start_lng, [2] end_lat, [3] end_lng
		loc_cursor = self.conn.cursor()
		result = []
		def_time = time.strftime("12:00:00")
		def_day = "TUE"
		query = """select _id from cloud where
					start_lat >  '%s' and
					start_lat <= '%s' and
					start_lng <  '%s' and
					start_lng >= '%s' and
					end_lat   <  '%s' and
					end_lat	  >= '%s' and
					end_lng   >  '%s' and
					end_lng   <= '%s' and
					start_time =  %s  and
					day = %s;
					"""
		try:
			loc_cursor.execute(query, (coords[2], coords[0],coords[3],coords[1],coords[0],coords[2],coords[1],coords[3],
									   def_time, def_day))
		except:
			raise Exception("Cloud doesnt exist")
		out = loc_cursor.fetchall()
		for i in range(0,len(out)):
			result.append(out[i][0])
		return result

	def custom_cloud(self, start_lat, start_lng, end_lat,end_lng):  #useless atm - needs to be fixed
		#10000% private method - user MUST NOT be able to access it under any circumstances !
		loc_cursor = self.conn.cursor()
		if self.coord_in_grid(start_lat, start_lng):
			if self.coord_in_grid(end_lat,
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

		return self.get_cloud_id(start_lat, start_lng, end_lat, end_lng)

	def insert_layer(self, layer, s_time, e_time, day):  # 5 layers - 0 to 4 (timestamp - time.strftime('22:00:00'))
		loc_cursor = self.conn.cursor()
		itr = int(math.pow(2, layer))
		for i in range(0, self.size_h - itr, itr):
			for j in range(0, self.size_w - itr, itr):
				query = """insert into cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time, layer, day)
														values ('%s','%s','%s','%s', %s, %s, '%s', %s)"""
				try:
					loc_cursor.execute(query,
									   (self.Matrix[i][j][0], self.Matrix[i][j][1], self.Matrix[i + itr][j + itr][0],
										self.Matrix[i + itr][j + itr][1], s_time, e_time, layer, day))
					self.conn.commit()
				except:
					self.conn.rollback()

	def get_layer_coords(self, layer):
		loc_cursor = self.conn.cursor()
		fetch = []
		if 0 <= layer < 5:
			query = """select start_lat, start_lng, end_lat, end_lng from cloud where layer = '%s'"""
			loc_cursor.execute(query, layer)
			fetch = loc_cursor.fetchall()
		return fetch
	#END: Cloud Table

	# START: Tweet_keyword table
	def insert_twt_keyword(self, tweet_id, kword):
		loc_cursor = self.conn.cursor()
		query = """insert into tweet_keyword (_tweet, _keyword) values ('%s', '%s')"""
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
		text = self.elim_useless(tweet['text'])
		day = self.parse_timestamp(tweet['time'])
		cloud = 0
		for each in text:
			self.insert_keyword(each)
		for layer in range(0, 5):
			cloud = self.point_in_cloud(tweet['lat'], tweet['lng'], day[0], day[1], layer)
		for each in text:
			for c in cloud:
				self.insert_counter(self.fetch_keyword_id(each), c)
		for each in text:
			self.insert_location(tweet['lat'], tweet['lng'], each)
		for each in text:
			self.insert_twt_keyword(tweet['_id'], each)

	def parse_timestamp(self, timestamp):
		day = timestamp[0:3].upper()
		t = timestamp[11:19]
		return day, t