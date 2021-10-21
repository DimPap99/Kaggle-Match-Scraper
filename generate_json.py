import json, os
os.path.dirname(__file__)
def generate_config(path):
    print(f"Will generate the configurations file in {path}")
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
   
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(CONFIGURATION, f, ensure_ascii=False, indent=4)
    return CONFIGURATION

def config_init(path):
    if os.path.exists(path):
        print("Detected configuration file...")
        with open(path, 'r') as f:
            conf = json.load(f)
        return conf
    else:
        print("Didnt find the configuration file, will generate an new one...")
        return generate_config(path)
