import json, os
os.path.dirname(__file__)

class Config_Handler(object):

    def __init__(self, json_path) -> None:
        self.config_path = json_path
        self.config = self.config_init()
        self.validate()
        self.target_competition = self.config["COMPETITIONS_INFO"]["TARGET_COMP"]
        self.url = self.config["URL"]

    def get_competition_id(self, competition):
        if competition in self.config["COMPETITIONS_INFO"]["COMP_IDS"].keys():
            return self.config["COMPETITIONS_INFO"]["COMP_IDS"][competition]
        else:
            print(f"A competition with the name {competition} doesnt exist inside the configuration file...")
            return None
    
    def get_directory(self, directory):
        if directory in self.config["DIRECTORIES"].keys():
            return self.config["DIRECTORIES"][directory]
        else:
            print(f"A directory with the name {directory} doesnt exist inside the configuration file...")
            return None
    
    def get_threshold(self, threshold:str):
        if threshold in self.config["THRESHOLDS"].keys():
            return self.config["THRESHOLDS"][threshold]
        else:
            print(f"A threshold with the name {threshold} doesnt exist inside the configuration file...")
            return None
    
    def config_init(self):
        if os.path.exists(self.config_path):
            print("Detected configuration file...")
            with open(self.config_path, 'r') as f:
                conf = json.load(f)
            
            return conf
        else:
            print("Didnt find the configuration file, will generate an new one...")
            return self.generate(self.config_path)
            
    def validate(self):
        if self.config["THRESHOLDS"]["MAX_REQUESTS"] > 3600:
            print("Fixed the max request threshold.")
            self.config["THRESHOLDS"]["MAX_REQUESTS"] = 3600
        
        if self.config["THRESHOLDS"]["REQUEST_INTERVAL"] < 1:
            print("Fixed the request interval.")
            self.config["THRESHOLDS"]["REQUEST_INTERVAL"] = 1

    def generate(self):
        print(f"Will generate the configurations file in {self.config_path}")
        CONFIGURATION = {}
        CONFIGURATION["COMPETITIONS_INFO"] = {

            "COMP_IDS": {
                'lux-ai-2021': 30067,
                'hungry-geese': 25401,
                'rock-paper-scissors': 22838,
                'santa-2020': 24539,
                'halite': 18011,
                'google-football': 21723
            },
            "TARGET_COMP": 'lux-ai-2021'
        }

        CONFIGURATION["THRESHOLDS"] = {
            "MAX_REQUESTS": 3600,
            "MIN_MATCH_SCORE": 1550,
            "REQUEST_INTERVAL": 1,
        }

        CONFIGURATION["URL"] = "https://www.kaggle.com/requests/EpisodeService/GetEpisodeReplay"

        CONFIGURATION["DIRECTORIES"] = {
            "KAGGLE_DATA": "./meta-kaggle/",
            "MATCHES": "./matches"
        }
        CONFIGURATION["FILTER_EPISODES"] = True 
    
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(CONFIGURATION, f, ensure_ascii=False, indent=4)
        return CONFIGURATION



        
