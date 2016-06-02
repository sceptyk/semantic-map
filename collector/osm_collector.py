from collector import Collector
from geopy.geocoders import Nominatim

class Osm_Collector(Collector):

	def authorize(self):
		return Nominatim()

	def get_data(self):
		return self.client.reverse('53.3093072,-6.2246967')
		print(location.raw)