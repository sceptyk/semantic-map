#!usr/bin/env python

from processor.cloud_parser import Cloud_Parser
import thread

parser = Cloud_Parser()

print("Cloud parser-----------------------")
thread.start_new_thread(parser.get_data, ())