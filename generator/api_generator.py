from collector.mysql_connect import Mysql_Connect
from cgi import escape
import time
import re
import simplejson as json

class Cloud_Generator(object):

	def __init__(self):
		self.db = Mysql_Connect()
		pass

	def generate(self, type = 'heatmap', filterValue = None):
		"""	@param type {string} = heatmap | cloud
			@param filterValue {Object} 
		"""
		
		filterValue = self._validateFilters(filterValue)

		print("Values filtered")
		print(json.dumps(filterValue))

		if type == 'heatmap':
			return self._get_heat_map(filterValue)

		elif type == 'grid':
			return self._get_grid_map(filterValue)

		elif type == 'cloud':
			return self._get_cloud(filterValue)

		elif type == 'popularity':
			return self._get_popularity(filterValue)

		elif type == 'movement':
			return self._get_global_movement(filterValue)

		else:
			raise Exception('Type of procedure is not defined')


	def _validateFilters(self, fv):
		"""Validate filters and set default values"""

		filters = {}

		#keywords
		keywords = json.loads(fv.get('k', '["%"]'))
		if len(keywords) > 5:
			raise Exception('Too many keywords')
		for i in range(len(keywords)):
			keyword = keywords[i]
			if len(keyword) > 10:
				raise Exception('Too long keyword')
			keywords[i] = escape(keyword)
		keywords = json.dumps(keywords).replace('[', '(').replace(']', ')')
		filters['keywords'] = keywords

		#boundary
		boundary = json.loads(fv.get('b', '{"north": 180, "south": -180, "west": -180, "east": 180}'))
		for key, value in boundary.items():
			try:
				value = float(value)
			except:
				raise Exception('Coordinates should be in range <-180, 180>')
			if value < -180 or value > 180:
				raise Exception('Incorrect coordinates')
			boundary[key] = float(value)
		filters['boundary'] = boundary


		#date
		date = json.loads(fv.get('d', '{"start": "2099-01-01 00:00:00", "end": "0000-00-00 00:00:00"}'))
		for key, value in date.items():
			try:
				re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', value)
			except:
				raise Exception('Date is not in format')
		filters['date'] = date

		#time
		time = json.loads(fv.get('t', '{"start": "00:00:00", "end": "24:00:00"}'))
		for key, value in time.items():
			if not re.match(r'\d{2}:\d{2}:\d{2}', value):
				raise Exception('Time is not in format')
		filters['time'] = time

		#days
		days = json.loads(fv.get('ds', '[1,2,3,4,5,6,7]'))
		if len(days) > 7:
			raise Exception('There is only 7 days in a week')
		for i in range(len(days)):
			day = days[i]
			try:
				day = int(day)
			except:
				raise Exception('Day index must be integer')
			if day < 1 or day > 7:
				raise Exception('Day index must be in range <1, 7>')
		days = json.dumps(days).replace('[', '(').replace(']', ')')
		filters['days'] = days

		#layer
		layer = json.loads(fv.get('l', '0'))
		try:
			layer = int(layer)
		except:
			raise Exception('Layer must be a number')
		if layer < 0 or layer > 4:
			raise Exception('Layer must be in range <0,4>')
		filters['layer'] = layer

		return filters

	def _get_heat_map(self, fv):
		"""Get last 10000 tweets' points
			@return array of squares"""

		sql_dev = """SELECT lat, lng 
			FROM tweets 
			WHERE 
				text IN %s 
				AND
				CAST(timestamp as TIME) > CAST('%s' as TIME) AND CAST(timestamp as TIME) < CAST('%s' as TIME)  
			ORDER BY timestamp DESC 
			LIMIT 10000""" % (fv['keywords'], fv['time']['start'], fv['time']['end'])

		#FIXME use parsed keywords

		return self._return_result(sql_dev)

	def _get_popularity(self, fv):
		"""Get hour from tweet date"""

		sql_dev = """SELECT HOUR(timestamp), count(HOUR(timestamp))
			FROM tweets 
			WHERE 
				text IN %s 
				AND
				CAST(timestamp as TIME) > CAST('%s' as TIME) AND CAST(timestamp as TIME) < CAST('%s' as TIME)  
			GROUP BY HOUR(timestamp)
			ORDER BY timestamp ASC 
			LIMIT 10000""" % (fv['keywords'], fv['time']['start'], fv['time']['end'])

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
			GROUP BY 
				c.start_lat, c.start_lng, c.end_lat, c.end_lng 
			ORDER BY
				weight DESC 
			LIMIT 10000""" % (fv['rect']['elt'], fv['rect']['eln'], fv['rect']['slt'], fv['rect']['sln'],
				fv['layer'], fv['days'], fv['time']['start'], fv['time']['end'])

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
				c.start_lat < '%s' AND c.start_lng < '%s' 
				AND
				c.end_lat > '%s' AND c.end_lng > '%s' 
				AND
				c.layer = '%s' 
				AND 
				c.day IN %s 
				AND 
				c.start_time > '%s' AND c.end_time < '%s' 
			GROUP BY
				k._id
			ORDER BY
				weight DESC 
			LIMIT 20""" % (fv['rect']['elt'], fv['rect']['eln'], fv['rect']['slt'], fv['rect']['sln'],
				fv['layer'], fv['days'], fv['time']['start'], fv['time']['end'])

		return self._return_result(sql_dev)

	def _get_global_movement(self, fv):
		"""Get movement chains of users
			@return array of points"""

		sql_dev = """SELECT x.lat, x.lng, y.lat, y.lng, z.lat, z.lng, x.user, CAST(MAX(x.timestamp) AS CHAR(19))
			FROM tweets x
			INNER JOIN (
				SELECT lat, lng, user, timestamp
			    FROM tweets
			    ORDER BY timestamp DESC
			) y ON x.user = y.user AND y.timestamp < x.timestamp AND y.timestamp > DATE_SUB(x.timestamp, INTERVAL 12 HOUR)

			LEFT JOIN (
				SELECT lat, lng, user, timestamp
			    FROM tweets
			    ORDER BY timestamp DESC
			) z ON x.user = z.user AND z.timestamp < y.timestamp AND z.timestamp > DATE_SUB(y.timestamp, INTERVAL 12 HOUR)
						
			WHERE x.lat < '%s' AND x.lng < '%s' AND x.lat > '%s' AND x.lng > '%s'
			GROUP BY x.user
			ORDER BY x.timestamp DESC
			LIMIT 20
			""" % (fv['rect']['elt'], fv['rect']['eln'], fv['rect']['slt'], fv['rect']['sln'])

		return self._return_result(sql_dev)

	def _return_result(self, sql):
		cursor = self.db.execute(sql)
		results = cursor.fetchall()

		return json.dumps(results, use_decimal=True)