#!usr/bin/env python
import processor.cloud_parser as pars
import generator.cloud_generator as gen
from collector.twitter_collector import Twitter_Collector
from collector.twitter_stream_collector import Twitter_Stream_Collector
import thread


#START: Initialize runners
twitter_stream = Twitter_Stream_Collector(
	client_key = 'd7zekGyp2jiMyd65ACGsfOECy',
	client_secret = 'xqiOuIlRFE9KWnYZJ7H85yFkzePVOJhEAT1o1dIQ18LKlLppw6',
	access_token = '737973216262639618-KPr0Fvuk6AUgsm8AjCTZbBhAe3fzLN4',
	access_secret = 'TQHGxJIQ8ze01jtsy9o65FwThvIK8JdHCsFnvWkNB5yfs')
connection = pars.Cloud_Parser()
#END: Initialize runners

#START: Initialize clouds
gen.Cloud_Generator(64,64)
#END: Initialize clouds

#START: Multi-threading
#thread.start_new_thread(twitter_stream.run(), ("Collector",1,))
#thread.start_new_thread(connection.get_data(), "Parser")
twitter_stream.run()