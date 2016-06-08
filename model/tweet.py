class Tweet(object):

    def __init__(self, tweet_id = 0, user_id = 0, text = '', lat = 0.0, lng = 0.0, time = ''):
        self._id = tweet_id
        self._user = user_id
        self.text = text
        self.lat = lat
        self.lng = lng
        self.time = time

    def dict(self):
        return {
            '_id'  : self._id,
            '_user': self._user,
            'text' : self.text,
            'lat'  : self.lat,
            'lng'  : self.lng,
            'time' : self.time
        }

    def tuple(self):
        return self._id, self._user, self.text, self.lat, self.lng, self.time

    def populate(self, list):
        self._id = list[0]
        self._user = list[1]
        self.text = list[2]
        self.lat = list[3]
        self.lng = list[4]
        self.time = list[5]
