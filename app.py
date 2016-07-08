#!/usr/bin/python
import os
import sys
from cherrypy import wsgiserver
import main_generator
import main_collector
#TODO import main_processor

#hack to make sure we can load wsgi.py as a module in this class
sys.path.insert(0, os.path.dirname(__file__))

virtenv = os.environ['OPENSHIFT_PYTHON_DIR'] + '/virtenv/'
virtualenv = os.path.join(virtenv, 'bin/activate_this.py')
try:
    execfile(virtualenv, dict(__file__=virtualenv))
except IOError:
    pass

# Get the environment information we need to start the server
ip = os.environ['OPENSHIFT_PYTHON_IP']
port = int(os.environ['OPENSHIFT_PYTHON_PORT'])
host_name = os.environ['OPENSHIFT_GEAR_DNS']

server = wsgiserver.CherryPyWSGIServer((ip, port), main_generator.application, server_name=host_name)
server.start()