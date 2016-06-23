#!/usr/bin/env python
import os

from wsgiref.simple_server import make_server
import json
from cgi import parse_qs, escape
from generator.cloud_generator import Cloud_Generator

STATIC_URL_PREFIX = '/'
STATIC_FILE_DIR = 'generator\public'

API_PREFIX = '/json'

MIME_TABLE = {
	'.txt': 'text/plain',
	'.html': 'text/html',
	'.css': 'text/css',
	'.js': 'application/javascript',
	'.ico': 'image/x-icon'
}

GENERATOR = Cloud_Generator()

def content_type(path):
	"""Return a guess at the mime type for this path based on the file extension"""

	name, ext = os.path.splitext(path)

	if ext in MIME_TABLE:
		return MIME_TABLE[ext]
	else:
		return "application/octet-stream"

def web_api(environ, start_response):
	"""Map urls to specific functions in cloud generator"""
	# response type json
	
	path = environ['PATH_INFO']
	path = path.replace(API_PREFIX, '') #remove api directory
	path = path[1:] #remove backslash from the begining
	path = os.path.normpath(path)

	d = parse_qs(environ['QUERY_STRING'])

	def GET(key):
		return escape(d.get(key, [''])[0])

	filter = GET('filter')
	filterValue = {
		'rect' : {
			'slt' : GET('slt'),
			'sln' : GET('sln'),
			'elt' : GET('elt'),
			'eln' : GET('eln'),
		},
		'time' : {
			'st' : GET('st'),
			'et' : GET('et'),
		},
		'keyword': GET('keyword'),
	}

	try:
		content = GENERATOR.generate(path, filter, filterValue)
	except Exception, e:
		print('Error %s' % str(e))
		return show_404_app(environ, start_response)

	headers = [('content-type', 'application/json')]
	start_response('200 OK', headers)
	return [content] 

def static_app(environ, start_response):
	"""Serve static files from the directory named in STATIC_FILES"""

	path = environ['PATH_INFO']
	path = STATIC_FILE_DIR + path
	path = os.path.normpath(path)

	# fall back to index first
	name, ext = os.path.splitext(path)
	if ext is None or not ext:
		path = path + '/index.html'

	if path.startswith(STATIC_FILE_DIR) and os.path.exists(path):

		h = open(path, 'rb')
		content = h.read()
		h.close()

		headers = [('content-type', content_type(path))]
		start_response('200 OK', headers)
		return [content]  

	else:
		return show_404_app(environ, start_response)

def show_404_app(environ, start_response):
	h = open(STATIC_FILE_DIR + '/error/404.html', 'rb')
	content = h.read()
	h.close()

	headers = [('content-type', 'html/text')]
	start_response('404 ERROR', headers)
	return [content]

def application(environ, start_response):
	"""WSGI application to switch between different applications
		based on the request URI"""

	path = environ['PATH_INFO']

	print(path)

	if path.startswith(API_PREFIX):
		return web_api(environ, start_response)
	else:
		return static_app(environ, start_response)

httpd = make_server('localhost', 8080, application)

print("Ready")
httpd.serve_forever()

#