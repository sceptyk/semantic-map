from geopy.geocoders import Nominatim
import json

class Osm_Helper(object):
	"Open Street Map layer component"

	def __init__(self):
		self.geolocator = Nominatim()

		self.types = {
			'aerialway'
		}
	
	def get_location(self, lat, lng):
		"""Query OSM for location details
			@return {class: string, type: string}"""
		position = "%f, %f" % (lat, lng)
		location = self.geolocator.reverse(position)
		more = self.geolocator.geocode(location.address)

		name = more.raw['display_name'].partition(',')[0]
		data = {'class': more.raw['class'], 'type': more.raw['type']}

		return data