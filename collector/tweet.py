class Tweet(object):
	'Row represantation of table tweets'

	def __init__(self, tweet_id, lat, lng, user, timestamp, text):
		self._id = tweet_id
		self.lat = lat
		self.lng = lng
		self.user = user
		self.timestamp = timestamp
		self.text = text

	def _dict(self):

		return {
			'_id' = self._id,
			'lat' = self.lat,
			'lng' = self.lng,
			'user' = self.user,
			'text' = self.text,
			'timestamp' = self.timestamp
		}

	def _touple(self):

		return (self._id, self.lat, self.lng, self.user, self.text, self.timestamp)