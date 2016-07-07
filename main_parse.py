import processor.cloud_parser as pars
import generator.cloud_generator as gen


connection = pars.Cloud_Parser()
enerator = gen.Cloud_Generator(64,64)
connection.get_data()
