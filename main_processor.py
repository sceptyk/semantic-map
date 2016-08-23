#!usr/bin/env python

from processor.cloud_parser import Cloud_Parser
import thread
import generator.cloud_generator as gen

gen.Cloud_Generator()
parser = Cloud_Parser()

if __name__ == '__main__':
	parser.get_data()
else:
	print("---------Cloud processor-----------")
	thread.start_new_thread(parser.get_data, ())