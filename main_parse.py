import processor.cloud_parser as pars
import generator.cloud_generator as gen


connection = pars.Cloud_Parser()
generator = gen.Cloud_Generator()
connection.get_data()