class Cloud_Parser(Object){
	
	def __init__:
		self.conn = MySQLdb.connect(
			host = '127.9.157.2', 
			user = 'adminw9XD8Ju',
			passwd = 'QIY_8QyGMW-X', 
			db = 'ucd')
		self.cursor = self.conn.cursor()

		CREATE_KEYWORDS_TABLE = """CREATE TABLE IF NOT EXISTS keywords (
			_id BIGINT UNSIGNED NOT NULL,
			keyword CHAR(100) NOT NULL,
			PRIMARY KEY ( _id )
		)"""
		self.cursor.execute(CREATE_KEYWORDS_TABLE)

		CREATE_CLOUD_TABLE = """CREATE TABLE IF NOT EXISTS cloud (
			_id BIGINT UNSIGNED NOT NULL,
			start_lat DOUBLE(12,7),
			start_lng DOUBLE(12,7),
			end_lat DOUBLE(12,7),
			end_lng DOUBLE(12,7),
			start_at TIME,
			end_at,
			PRIMARY KEY ( _id )
		)"""
		self.cursor.execute(CREATE_CLOUD_TABLE)

		CREATE_COUNTER_TABLE = """CREATE TABLE IF NOT EXISTS word_counter (
			_id BIGINT UNSIGNED NOT NULL,
			_keyword BIGINT UNSIGNED NOT NULL,
			_cloud BIGINT UNSIGNED NOT NULL,
			count BIGINT UNSIGNED NOT NULL,
			PRIMARY KEY ( _id )
		)"""
		self.cursor.execute(CREATE_COUNTER_TABLE)
	
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
		    
		tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
		emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)
		 
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
	
	def store_data(self, data):
		pass
		#TODO update each branch
		# -- each place based on location
		# -- each time part
		# -- global list

}