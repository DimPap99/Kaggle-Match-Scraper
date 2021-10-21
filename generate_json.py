import json


CONFIGURATION = {}

CONFIGURATION["Competitions_Info"] = {

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
    "MIN_MATCH_sCORE": 1550,
    "REQUEST_INTERVAL": 1,
}

CONFIGURATION["URL"] = "https://www.kaggle.com/requests/EpisodeService/GetEpisodeReplay"

CONFIGURATION["DIRECTORIES"] = {
    "KAGGLE_DATA": "./input/meta-kaggle/",
    "MATCHES": "./matches"
}