class Tweet(object):

    def __init__(self, tweet_id, user_id, text, lat, lng, time):
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
