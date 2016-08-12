#!usr/bin/env python

from processor.cloud_parser import Cloud_Parser
import thread

parser = Cloud_Parser()

if __name__ == '__main__':
	import generator.cloud_generator as gen
	gen.Cloud_Generator()
else:
	print("---------Cloud processor-----------")
	thread.start_new_thread(parser.get_data, ())