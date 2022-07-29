import json
from data_gen import DataGenerator

class DataGenClient(object):
    ### Constructor
    def __init__(self,generation_json):
        gen_dict = json.load(open(generation_json,'r'))
        self.generator_config = gen_dict
   

    ### Generate Data
    def generate_data(self):
        data_gen = DataGenerator(self.generator_config)
        data_gen.generate_data()



if __name__ == "__main__":
    generate_json = "C:\\source\\tsdbsbenchmark\\Python\\data_generation.json"
    datagen_client = DataGenClient(generate_json)
    datagen_client.generate_data()