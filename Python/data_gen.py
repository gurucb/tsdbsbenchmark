import json


class DataGenerator(object):

    def __init__(self,generator_json) -> None:
        self.generator_config = generator_json
        
    

    def generate_data(self):
        gen_config = self.generator_config
        payloads = gen_config.get("load_details").get("paylaod_json_list")
        payload_json = json.load(open(payloads[0],'r'))
        print("Generator Config")
        print(gen_config)
        print("Payload Config")
        print(payload_json)