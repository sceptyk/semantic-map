import json

class Node(object):

	def __init__(self, _id, coordinates, location, timestamp, keywords):
		self._id = _id
		self.coordinates = coordinates
		self.location = location
		self.timestamp = timestamp
		self.keywords = keywords

	def to_json(self):

		return json.dumps({
			'id': self._id,
			'coordinates': self.coordinates,
			'location': self.location,
			'timestamp': self.timestamp,
			'keywords': self.keywords
		}, sort_keys=True)