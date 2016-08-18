import math
import time

class Util(object):
    def __init__(self):
        pass

    def _to_degrees(self, km):
        """Convert kms to degrees
            FIXME more precise calculations"""
        return (km / 40000) * 360

    def _to_64(self, n):
        digits = [
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U',
            'V', 'W', 'X', 'Y', 'Z',
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
            'v', 'w', 'x', 'y', 'z',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
        ]
        encoded = ''
        while n:
            encoded = digits[n % 64] + encoded
            n /= 64

        return encoded

    def to_deg_lat(self, km):
        return km / 111

    def to_deg_lng(self, lat, km):
        return abs(km / (111 * math.cos(lat)))

    def hash_geo(self, lat, lng, precision):
        """Geo hash coordinates"""

        # move coordinates according to (0,0)
        lat += 180
        lng += 180

        # bring coordinates to precision
        degs = self._to_degrees(precision)
        #deg_lat = self.to_deg_lat(precision)
        #deg_lng = self.to_deg_lng(int(lat), precision)

        lat = int(lat / degs)
        lng = int(lng / degs)

        # calculate index
        cols = int(360 / degs)
        index = (lat * cols) + lng

        # encode base64
        hash = self._to_64(index)

        # return {string}
        return hash

    def day_time(self, t):
        if '00:00:00' <= t < '04:00:00': return 0
        if '04:00:00' <= t < '12:00:00': return 1
        if '12:00:00' <= t < '17:00:00': return 2
        if '17:00:00' <= t < '22:00:00': return 3
        if '22:00:00' <= t <='24:00:00': return 4
        
        else: return 5

    def day_time_array(self, st, et):
        start = self.day_time(st)
        end = self.day_time(et)

        return [i for i in range(start, end+1)]

    precisions = [0, 0.2, 1.0, 5.0, 25.0, 100.0]

    def layer_index(self, precision):
        return Util.precisions.index(precision)

    def layer_precision(self, index):
        return Util.precisions[index]