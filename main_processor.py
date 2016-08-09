#!usr/bin/env python

from processor.cloud_parser import Cloud_Parser as pars
import thread
import generator.cloud_generator as gen

gen.Cloud_Generator()
pars.Cloud_Parser()
