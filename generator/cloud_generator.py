from collector.mysql_connect import Mysql_Connect
import json

class Cloud_Generator(object):

	def __init__(self):
		self.db = Mysql_Connect()

	def generate(self, type = 'heatmap', filter = 'global', filterValue = None):
		"""	@param type {string} = heatmap | cloud
			@param filter {string} = location | global
			@param filterValue {any} = boundary | span | None
			"""

		#validate filterValue
		# filter
		# rect (slt, sln, elt, eln)
		# time (st, et)

		if type == 'heatmap':
			if filter == 'global':
				return self._get_global_heat_map(filterValue)
			elif filter == 'location':
				return self._get_zoomed_heat_map(filterValue)

		elif type == 'cloud':
			if filter == 'global':
				return self._get_global_cloud(filterValue)
			elif filter == 'location':
				return self._get_zoomed_cloud(filterValue)

		elif type == 'movement':
			return self._get_global_movement()

		else:
			raise Exception('Type of procedure is not defined')


	def _get_global_heat_map(self, fv):
		"""Get weight of each square cloud
			@return array of squares"""
		
		sql = """SELECT SUM(wc.count), c.start_lat, c.start_lng, c.end_lat, c.end_lng FROM word_counter wc
			GROUP BY wc._cloud 
			ORDER BY wc._cloud DESC
			INNER JOIN keywords k 
				ON k._id = wc._keyword 
			INNER JOIN cloud c
				ON c._id = wc._cloud
			WHERE 
				(k.word LIKE %s) 
				AND 
				(c.start_time > %s AND c.end_time < %s)
			""" % (fv['keyword'], fv['time']['st'], fv['time']['et'])

		sql_dev = """SELECT lat, lng FROM tweets WHERE lat <> 0 LIMIT 10000""";

		return self._return_result(sql_dev)
		#return self._return_result(sql)

	def _get_zoomed_heat_map(self, filterValue):
		"""Get points to display proper heatmap
			@return array of points"""

		sql = """SELECT t.lat, t.lng FROM tweets t
			INNER JOIN tweet_keywords tk 
				ON tk._tweet = t._id 
			INNER JOIN keywords k 
				ON k._id = tk._keyword 
			INNER JOIN word_cloud wc 
				ON k._id = wc._keyword
			INNER JOIN cloud c 
				ON wc._keyword = c._id 
			WHERE 
				(k.word LIKE %s) 
				AND 
				(c.start_time > %s AND c.end_time < %s)
				AND 
				(c.start_lat < %f AND c.start_lng < %f AND c.end_lat < %f AND c.end_lng < %f)
			ORDER BY wc.count 
			LIMIT 10000
			""" % filterValue

		return self._return_result(sql)

	def _get_global_cloud(self, filterValue):
		"""Get keywords of global cloud
			@return array of keywords"""

		sql = """SELECT k.word, SUM(wc.count) as count FROM keywords k
			INNER JOIN word_count wc 
				ON k._id = wc._keyword 
			GROUP BY wc._keyword 
			ORDER BY count 
			WHERE 
				(c.start_time > %s AND c.end_time < %s) 
			LIMIT 20
			""" % filterValue

		return self._return_result(sql)


	def _get_zoomed_cloud(self, filterValue):
		"""Get keywords of global cloud
			@return array of keywords"""

		sql = """SELECT k.word, SUM(wc.count) as count FROM keywords k
			INNER JOIN word_count wc 
				ON k._id = wc._keyword 
			GROUP BY wc._keyword 
			ORDER BY count 
			WHERE 
				(c.start_time > %s AND c.end_time < %s)
				AND 
				(c.start_lat < %f AND c.start_lng < %f AND c.end_lat < %f AND c.end_lng < %f)
			LIMIT 20
			""" % filterValue

		return self._return_result(sql)

	def _get_global_movement(self):
		"""Get movement chains of users
			@return array of points"""

		sql = """SELECT user, lat, lng FROM tweets
			WHERE user IN (
				SELECT t.user FROM tweets AS t
			    GROUP BY t.user
			    HAVING count(t.user) > 100
			)
			ORDER BY user;
			""" % filterValue

		return self._return_result(sql)

	def _return_result(self, sql):
		cursor = self.db.execute(sql)

		results = cursor.fetchall()

		print(sql)

		return json.dumps(results)