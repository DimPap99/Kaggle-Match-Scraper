import os
from Config_Handler import Config_Handler
from get_matches import get_matches
script_dir = os.path.dirname(__file__)
config_path = os.path.join(script_dir, "conf.json")
config_handler = Config_Handler(config_path) 


EPISODES_PATH = os.path.join(config_handler.get_directory("KAGGLE_DATA"), "Episodes.csv")
FILTERED_EPISODES_PATH = os.path.join(config_handler.get_directory("KAGGLE_DATA"), "Filtered_episodes.csv")
EPISODE_AGENTS_PATH = os.path.join(config_handler.get_directory("KAGGLE_DATA"), "EpisodeAgents.csv")
MIN_SCORE = config_handler.get_threshold("MIN_MATCH_SCORE")

#TODO: ADD ERROR HANDLING WHEN KAGGLE BANS OUR IP
#TODO: ADD A WAY TO SAVE THE AMOUNT OF DAILY REQUESTS 
if __name__ ==  '__main__':
    get_matches(EPISODES_PATH, FILTERED_EPISODES_PATH, EPISODE_AGENTS_PATH, MIN_SCORE, config_handler)
