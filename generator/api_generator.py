from collector.mysql_connect import Mysql_Connect
from util.util import Util
from cgi import escape
import time
import re
import simplejson as json

class Cloud_Generator(object):

	def __init__(self):
		self.db = Mysql_Connect()
		self.util = Util()
		

	def generate(self, type = 'heatmap', filterValue = None):
		"""	@param type {string} = heatmap | cloud
			@param filterValue {Object} 
		"""
		
		#TODO request limit per IP

		filterValue = self._validate_filters(filterValue)

		#print("Values filtered")
		#print(json.dumps(filterValue))

		if type == 'heatmap':
			return self._get_heat_map(filterValue)

		elif type == 'grid':
			return self._get_grid_map(filterValue)

		elif type == 'cloud':
			return self._get_cloud(filterValue)

		elif type == 'popularity':
			return self._get_popularity(filterValue)

		elif type == 'grid_popularity':
			return self._get_grid_popularity(filterValue)

		else:
			raise Exception('Type of procedure is not defined')


	def _validate_filters(self, fv):
		"""Validate filters and set default values"""

		filters = {}

		#keywords
		keywords = json.loads(fv.get('k', '["(.*)"]'))
		regex = ''
		if len(keywords) > 5:
			raise Exception('Too many keywords')
		for i in range(len(keywords)):
			keyword = keywords[i]
			if len(keyword) > 10:
				raise Exception('Too long keyword')
			regex += escape(keyword) + '|'
		keywords = regex[:-1]
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
		date = json.loads(fv.get('d', '{"start": "0000-00-00 00:00:00", "end": "2099-01-01 00:00:00"}'))
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
		filters['daytime'] = {'start': self.util.day_time(time['start']), 'end': self.util.day_time(time['end'])}

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

		#geohash
		center = json.loads(fv.get('c', '[53.3385255,-6.2473989]'))
		filters['geohash'] = self.hash(center[0], center[1], self.util.layer_precision(layer))
		

		return filters

	def _get_heat_map(self, fv):
		"""Get last 10000 tweets' points
			@return array of squares"""

		sql_dev = """SELECT lat, lng 
			FROM tweets 
			WHERE 
				text REGEXP '%s'
				AND
				CAST(timestamp as TIME) > CAST('%s' as TIME) AND CAST(timestamp as TIME) < CAST('%s' as TIME)  
			ORDER BY timestamp DESC 
			LIMIT 2000""" % (fv['keywords'], fv['time']['start'], fv['time']['end'])
			#LIMIT 10000
		#FIXME use parsed keywords

		return self._return_result(sql_dev)

	def _get_popularity(self, fv):
		"""Get hour from tweet date"""

		sql_dev = """SELECT HOUR(timestamp), count(HOUR(timestamp))
			FROM tweets 
			WHERE 
				text REGEXP '%s' 
				AND
				CAST(timestamp as TIME) > CAST('%s' as TIME) AND CAST(timestamp as TIME) < CAST('%s' as TIME)  
			GROUP BY HOUR(timestamp)
			ORDER BY HOUR(timestamp) ASC 
			LIMIT 2000""" % (fv['keywords'], fv['time']['start'], fv['time']['end'])

		return self._return_result(sql_dev)

	def _get_grid_popularity(self, fv):
		"""Get day time frequency from word counters"""
		
		sql_dev = """SELECT wc.day_time, sum(wc.count) FROM word_counter wc
			WHERE
			    wc._layer = '1'
			GROUP BY wc._cloud , wc._layer, wc.day_time
			ORDER BY wc._layer DESC"""

		return self._return_result(sql_dev)

	def _get_grid_map(self, fv):
		"""Get most word clouds grid"""

		sql_dev = """SELECT wc._cloud, sum(wc.count) FROM word_counter wc
			WHERE
			    wc._layer = '%s'
			GROUP BY wc._cloud , wc._layer
			ORDER BY sum(wc.count) DESC
			LIMIT 10000""" % (fv['layer'],)

		return self._return_result(sql_dev)

	def _get_cloud(self, fv):
		"""Get keywords of global cloud
			@return array of keywords"""

		sql_dev = """SELECT k.word, sum(wc.count) FROM word_counter wc
			        INNER JOIN
			    keywords k ON k._id = wc._keyword
			WHERE
			    wc._cloud = '%s'
			    AND wc._layer = '%s'
			    AND wc.day_time >= '%s'
			    AND wc.day_time <= '%s'
			GROUP BY wc._keyword , wc._cloud , wc._layer
			ORDER BY sum(wc.count) DESC
			LIMIT 20""" % (fv['geohash'], fv['layer'], fv['daytime']['start'], fv['daytime']['end'])

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

	def _get_activity_chain(self, fv):
		"""Get movement chains of users
			@return array of points"""

		sql_dev = """pass""" 

		return self._return_result(sql_dev)

	def _return_result(self, sql):
		cursor = self.db.execute(sql)
		results = cursor.fetchall()

		return json.dumps(results, use_decimal=True)