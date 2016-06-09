#!/usr/bin/env python
import os

virtenv = os.environ['OPENSHIFT_PYTHON_DIR'] + '/virtenv/'
virtualenv = os.path.join(virtenv, 'bin/activate_this.py')
try:
    execfile(virtualenv, dict(__file__=virtualenv))
except IOError:
    pass
#
# IMPORTANT: Put any additional includes below this line.  If placed above this
# line, it's possible required libraries won't be in your searchable path
#
from collector.twitter_collector import Twitter_Collector
from collector.twitter_stream_collector import Twitter_Stream_Collector

from threading import Thread
import time
import tweepy

twitter_stream = Twitter_Stream_Collector(
    client_key = 'd7zekGyp2jiMyd65ACGsfOECy',
    client_secret = 'xqiOuIlRFE9KWnYZJ7H85yFkzePVOJhEAT1o1dIQ18LKlLppw6',
    access_token = '737973216262639618-KPr0Fvuk6AUgsm8AjCTZbBhAe3fzLN4',
    access_secret = 'TQHGxJIQ8ze01jtsy9o65FwThvIK8JdHCsFnvWkNB5yfs')

print("Twitter data ------")

twitter_stream.run()

def application(environ, start_response):

    ctype = 'text/plain'
    if environ['PATH_INFO'] == '/health':
        response_body = "1"
    elif environ['PATH_INFO'] == '/env':
        response_body = ['%s: %s' % (key, value)
                    for key, value in sorted(environ.items())]
        response_body = '\n'.join(response_body)
    else:
        ctype = 'text/html'
        response_body = '''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1">
  <title>Semantic map 2016 @ ucd</title>

</head>
<body>
<h1>Authors:</h1>
<ul>
<li>Vsevolods Caka</li>
<li>Filip Nowakowski</li>
</ul>
<p>Summer 2016</p>
</body>
</html>'''

    status = '200 OK'
    response_headers = [('Content-Type', ctype), ('Content-Length', str(len(response_body)))]
    #
    start_response(status, response_headers)
    return [response_body]


#
# Below for testing only
#
if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    httpd = make_server('localhost', 8051, application)
    # Wait for a single request, serve it and quit.
    httpd.handle_request()
