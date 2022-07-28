import json


class DataGenClient(object):
    ### Constructor
    def __init__(self):
        pass
    ### Building Configuration for DataGeneratorClient
    def build_config(self,generation_json):
            gen_dict = json.load(open(generation_json,'r'))

    ### Generate Data
    def generate_data(self):
        pass



if __name__ == "__main__":
    generate_json = "C:\\source\\tsdbsbenchmark\\Python\\data_generation.json"
    datagen_client = DataGenClient()
    datagen_client.build_config(generation_json=generate_json)