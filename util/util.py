class Util(object):
	"""Helper class"""

	def __init__(self):
		pass

	def _to_degrees(self, km):
		"""Convert kms to degrees
			FIXME more precise calculations"""
		return (km/40000)*360;

	def _to_64(self, n):

		digits = [
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',	
			'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
		]

		encoded = ''
		while n:
			encoded = digits[n % 64] + encoded
			n /= 64

		return encoded

	def hash(self, lat, lng, precision):
		"""Geo hash coordinates"""
		
		#move coordinates according to (0,0)
		lat += 180
		lng += 180

		#bring coordinates to precision
		degs = self._to_degrees(precision)
		lat = int(lat / degs)
		lng = int(lng / degs)

		#calculate index
		cols = int(360 / degs);
		index = (lat * cols) + lng

		#encode base64
		hash = self._to_64(index)

		#return {string}
		return hash

	def day_time(self, t):
		if time.strftime('04:00:00') <= t <= time.strftime('11:59:59') : return 1
		if time.strftime('12:00:00') <= t <= time.strftime('16:59:59'): return 2
		if time.strftime('17:00:00') <= t <= time.strftime('21:59:59'): return 3
		if time.strftime('22:00:00') <= t <= time.strftime('03:59:59') : return 4
		else: return 0

	def layer_index(self, precision):
		if precision == 0.2:
			return 1
		elif precision == 0.6:
			return 2
		elif precision == 1.2:
			return 3
		else: return 0

	def layer_precision(self, index):
		precisions = [0, 0.2, 0.6, 1.2]
		return precisions[index]