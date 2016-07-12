from collector.mysql_connect import Mysql_Connect
import json

class Cloud_Generator(object):

	def __init__(self):
		#self.db = Mysql_Connect()
		pass

	def generate(self, type = 'heatmap', filterValue = None):
		"""	@param type {string} = heatmap | cloud
			@param filter {string} = location | global
			@param filterValue {any} = boundary | span | None
			"""

		#validate filterValue
		# filter
		# rect (slt, sln, elt, eln)
		# time (st, et)

		if type == 'heatmap':
			return self._get_heat_map(filterValue)

		elif type == 'grid':
			return self._get_grid_map(filterValue)

		elif type == 'cloud':
			return self._get_cloud(filterValue)

		elif type == 'popularity':
			return self._get_popularity(filterValue)

		elif type == 'movement':
			return self._get_global_movement()

		else:
			raise Exception('Type of procedure is not defined')


	def _get_heat_map(self, fv):
		"""Get last 10000 tweets' points
			@return array of squares"""
		
		sql_dev = """SELECT lat, lng 
			FROM tweets 
			WHERE 
				text LIKE '%%%s%%' 
				AND
				CAST(timestamp as TIME) > CAST('%s' as TIME) AND CAST(timestamp as TIME) < CAST('%s' as TIME)  
				AND
				timestamp > '%s' 
			ORDER BY timestamp DESC 
			LIMIT 10000""" % (fv['keyword'], fv['time']['start'], fv['time']['end'], fv['time']['recent'])

		return self._return_result(sql_dev)

	def _get_popularity(self, fv):
		"""Get hour from tweet date"""

		sql_dev = """SELECT HOUR(timestamp)  
			FROM tweets 
			WHERE 
				text LIKE '%%%s%%' 
				AND
				CAST(timestamp as TIME) > CAST('%s' as TIME) AND CAST(timestamp as TIME) < CAST('%s' as TIME)  
				AND
				timestamp > '%s' 
			ORDER BY timestamp DESC 
			LIMIT 10000""" % (fv['keyword'], fv['time']['start'], fv['time']['end'], fv['time']['recent'])

		return self._return_result(sql_dev)

	def _get_grid_map(self, fv):

		sql_dev = """SELECT c.start_lat, c.start_lng, c.end_lat, c.end_lng, SUM(count) as weight 
			FROM word_counter wc 
			INNER JOIN cloud c 
				ON c._id = wc._cloud 
			WHERE 
				c.start_lat < '%s' AND c.start_lng < '%s' 
				AND
				c.end_lat > '%s' AND c.end_lng > '%s' 
				AND
				c.layer = '%s' 
				AND 
				c.day IN %s 
				AND 
				c.start_time > '%s' AND c.end_time < '%s' 
			GROUP BY c.start_lat, c.start_lng, c.end_lat, c.end_lng
			LIMIT 10000"""
			#TODO pass args

		return self._return_result(sql_dev)

	def _get_cloud(self, fv):
		"""Get keywords of global cloud
			@return array of keywords"""

		sql_dev = """SELECT k.word, SUM(wc.count) as weight FROM keywords k
			INNER JOIN word_counter wc 
				ON wc._keyword = k._id
			INNER JOIN cloud c
				ON wc._cloud = c._id
			WHERE
				c.start_lat > '%s' AND c.start_lng > '%s'
			    AND
			    c.end_lat < '%s' AND c.end_lng < '%s'
			GROUP BY
				k._id
			ORDER BY
				weight DESC
			LIMIT 10000"""
			#pass args

		return self._return_result(sql_dev)

	def _get_global_movement(self):
		"""Get movement chains of users
			@return array of points"""

		sql_dev = """SELECT user, lat, lng 
			FROM tweets 
			ORDER BY user 
			LIMIT 10000;"""

		return self._return_result(sql_dev)

	def _return_result(self, sql):
		db = Mysql_Connect()
		cursor = db.execute(sql)

		results = cursor.fetchall()
		db.close()
		#print(sql)

		return json.dumps(results)