from collector.mysql_connect import Mysql_Connect
from util.util import Util
from cgi import escape
import time
import re
import simplejson as json

class Api_Generator(object):

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

		elif type == 'cloud':
			return self._get_cloud(filterValue)

		elif type == 'popularity':
			return self._get_popularity(filterValue)

		else:
			raise Exception('Type of procedure is not defined')


	def _validate_filters(self, fv):
		"""Validate filters and set default values"""

		def __array_to_list(arr):
			return json.dumps(arr).replace('[', '(').replace(']', ')')

		filters = {}
		#keywords
		keywords = json.loads(fv.get('k', ['[""]'])[0])
		regex = ''
		if len(keywords) > 5:
			raise Exception('Too many keywords')
		for i in range(len(keywords)):
			keyword = keywords[i]
			if len(keyword) == 0:
				regex = "(.*)|"
				break
			if len(keyword) > 10:
				raise Exception('Too long keyword')
			regex += escape(keyword) + '|'
		filters['regwords'] = regex[:-1]
		filters['keywords'] = __array_to_list(keywords)

		#boundary
		boundary = json.loads(fv.get('b', ['{"north": 180, "south": -180, "west": -180, "east": 180}'])[0])
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
		date = json.loads(fv.get('d', ['["2016-01-01 00:00:00", "2099-01-01 00:00:00"]'])[0])
		for value in date:
			try:
				re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', value)
			except:
				raise Exception('Date is not in format')
			if value < '2016-01-01 00:00:00':
				raise Exception('Date out of limit')
		if not date[0] < date[1]:
			raise Exception('Dates must not be the same')
		filters['date'] = {"start": date[0], "end": date[1]}

		#time
		time = json.loads(fv.get('t', ['["00:00:00", "24:00:00"]'])[0])
		for value in time:
			if not re.match(r'\d{2}:\d{2}:\d{2}', value):
				raise Exception('Time is not in format')
		filters['time'] = {"start": time[0], "end": time[1]}

		#daytime
		filters['daytime'] = __array_to_list(self.util.day_time_array(time[0], time[1]))

		#days
		days = json.loads(fv.get('ds', ['[1,2,3,4,5,6,7]'])[0])
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
		days = __array_to_list(days)
		filters['days'] = days

		#layer
		layer = json.loads(fv.get('l', ['4'])[0])
		try:
			layer = int(layer)
		except:
			raise Exception('Layer must be a number')
		if layer < 1 or layer > 4:
			raise Exception('Layer must be in range <1,4>')
		filters['layer'] = layer

		#geohash
		center = json.loads(fv.get('c', ['[53.3385255,-6.2473989]'])[0])
		filters['cloud'] = self.util.hash_geo(center[0], center[1], self.util.layer_precision(layer))
		filters['parent'] = self.util.hash_geo(center[0], center[1], self.util.layer_precision(layer+1))

		#details
		details = json.loads(fv.get('dl', ['false'])[0])
		filters['details'] = bool(details)

		#print json.dumps(filters)

		return filters

	def _get_heat_map(self, fv):
		"""Get last 10000 tweets' points
			@return array of squares"""

		if fv['details']:
			sql_dev = """SELECT 
				    wc._cloud, SUM(wc.count) AS counter
				FROM
				    word_counter wc
				    INNER JOIN keywords k
						ON k._id = wc._keyword
				WHERE
				    wc._layer = 1 AND 
				    wc.day IN %s AND 
				    wc.day_time IN %s AND 
				    k.word IN %s
				GROUP BY wc._cloud
				ORDER BY counter DESC
				LIMIT 1000""" % (fv['days'], fv['daytime'], fv['keywords'])
		else:
			sql_dev = """SELECT lat, lng 
				FROM tweets 
				WHERE 
					text REGEXP '%s'
					AND
					CAST(timestamp as TIME) > CAST('%s' as TIME) AND CAST(timestamp as TIME) < CAST('%s' as TIME) AND
					timestamp > '%s' AND timestamp < '%s' 
				ORDER BY timestamp DESC 
				LIMIT 5000""" % (fv['regwords'], fv['time']['start'], fv['time']['end'], fv['date']['start'], fv['date']['end'])
				#LIMIT 10000
			#FIXME use parsed keywords

		return self._return_result(sql_dev)

	def _get_popularity(self, fv):
		"""Get hour from tweet date"""

		if fv['details']:
			sql_dev = """SELECT 
				    wc.day_time, SUM(wc.count) AS counter
				FROM
				    word_counter wc
				    INNER JOIN keywords k
						ON k._id = wc._keyword
				WHERE
				    wc._layer = 1 AND 
				    wc.day IN %s AND 
				    wc.day_time IN %s AND 
				    k.word IN %s
				GROUP BY wc.day_time
				ORDER BY wc.day_time ASC""" % (fv['days'], fv['daytime'], fv['keywords'])
		else:
			sql_dev = """SELECT HOUR(timestamp), count(HOUR(timestamp))
				FROM tweets 
				WHERE 
					text REGEXP '%s' AND
					CAST(timestamp as TIME) > CAST('%s' as TIME) AND CAST(timestamp as TIME) < CAST('%s' as TIME) AND 
					timestamp > '%s' AND timestamp < '%s'
				GROUP BY HOUR(timestamp)
				ORDER BY HOUR(timestamp) ASC""" % (fv['regwords'], fv['time']['start'], fv['time']['end'], fv['date']['start'], fv['date']['end'])


		return self._return_result(sql_dev)

	def _get_cloud(self, fv):
		"""Get keywords of global cloud
			@return array of keywords"""

		sql_dev = """SELECT k.word, SUM(wc.count) AS counter FROM word_counter wc
			INNER JOIN keywords k ON k._id = wc._keyword
			WHERE
			    wc._cloud = '%s'
                AND wc._keyword <> 1
			group by wc._keyword
			ORDER BY counter DESC
			LIMIT 20""" % (fv['cloud'],)# fv['daytime'], fv['days'])

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