#!/usr/bin/env python
import os

import json
from cgi import parse_qs, escape
from generator.api_generator import Api_Generator

STATIC_FILE_DIR = os.path.normpath('generator/public')

API_PREFIX = os.path.normpath('/json')

MIME_TABLE = {
	'.txt': 'text/plain',
	'.html': 'text/html',
	'.css': 'text/css',
	'.js': 'application/javascript',
	'.ico': 'image/x-icon'
}

GENERATOR = Api_Generator()

def content_type(path):
	"""Return a guess at the mime type for this path based on the file extension"""

	name, ext = os.path.splitext(path)

	if ext in MIME_TABLE:
		return MIME_TABLE[ext]
	else:
		return "application/octet-stream"

def get_static_path(path):
	path = path[1:]
	path = os.path.join(STATIC_FILE_DIR, path)
	path = os.path.normpath(path)

	# fall back to index first
	name, ext = os.path.splitext(path)
	if ext is None or not ext:
		path = path + '/index.html'

	path = os.path.join(os.environ.get('OPENSHIFT_REPO_DIR', ''), path)
	return path

def web_api(environ, start_response):
	"""Map urls to specific functions in cloud generator"""
	# response type json
	
	path = environ['PATH_INFO']
	path = os.path.normpath(path)
	#print("WEB API__________________")
	path = path.replace(API_PREFIX, '') #remove api directory
	path = path[1:] #remove slash from the begining
	d = parse_qs(environ['QUERY_STRING'])

	try:
		content = GENERATOR.generate(path, d)
	except Exception, e:
		print('Error %s' % str(e))
		return show_404_app(environ, start_response)

	headers = [('content-type', 'application/json')]
	start_response('200 OK', headers)
	return [content] 

def static_app(environ, start_response):
	"""Serve static files from the directory named in STATIC_FILE_DIR (/public)"""

	path = environ['PATH_INFO']
	path = get_static_path(path)

	#print("STATIC APP _________________")
	#print(path)
	#print(os.path.exists(path))

	if os.path.exists(path):

		h = open(path, 'rb')
		content = h.read()
		h.close()

		headers = [('content-type', content_type(path))]
		start_response('200 OK', headers)
		return [content]  

	else:
		return show_404_app(environ, start_response)

def show_404_app(environ, start_response):

	path = get_static_path('/error/404.html')

	h = open(path, 'rb')
	content = h.read()
	h.close()

	headers = [('content-type', 'html/text')]
	start_response('404 ERROR', headers)
	return [content]

def application(environ, start_response):
	"""WSGI application to switch between different applications
		based on the request URI"""

	path = environ['PATH_INFO']
	path = os.path.normpath(path)

	if path.startswith(API_PREFIX):
		return web_api(environ, start_response)
	else:
		return static_app(environ, start_response)

if __name__ == '__main__':
	from wsgiref.simple_server import make_server
	httpd = make_server('localhost', 8081, application)

	print("Server ready")
	httpd.serve_forever()