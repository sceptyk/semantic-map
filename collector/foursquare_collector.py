from collector import Collector
import foursquare

class Foursquare_Collector(Collector):
	
	def authorize(self):
		return foursquare.Foursquare(self._CLIENT_KEY, self._CLIENT_SECRET)

	def process():
		pass

	def get_data(self):
		return self.client.venues.search({'ll': '53.3095642,-6.2245867,17'})