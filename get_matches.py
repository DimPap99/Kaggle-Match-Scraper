import pandas as pd
import numpy as np
import os
import requests
import json
import datetime
import time
import glob, sys
import collections
from helper import *
from process import *
from generate_json import generate_config, config_init

script_dir = os.path.dirname(__file__)
config_path = os.path.join(script_dir, "conf.json")
conf_json = config_init(config_path)
EPISODES_PATH = os.path.join(conf_json["DIRECTORIES"]["KAGGLE_DATA"], "Episodes.csv")
EPISODE_AGENTS_PATH = os.path.join(conf_json["DIRECTORIES"]["KAGGLE_DATA"], "EpisodeAgents.csv")
TARGET_COMPETITION = conf_json["COMPETITIONS_INFO"]["COMP_IDS"]



if can_be_loaded(EPISODES_PATH) is True:
    print("Will load episodes...")
    # Load Episodes
    episodes_df = pd.read_csv(EPISODES_PATH)
    print(f'Episodes.csv: {len(episodes_df)} rows before filtering.')
    episodes_df = episodes_df[episodes_df.CompetitionId == conf_json["COMPETITIONS_INFO"]] 
    print(f'Filtered based on the CompetitionId...')
    episodes_df = episodes_df.set_index(['Id'])
    episodes_df['CreateTime'] = pd.to_datetime(episodes_df['CreateTime'])
    episodes_df['EndTime'] = pd.to_datetime(episodes_df['EndTime'])
    print(f'Episodes.csv: {len(episodes_df)} rows after filtering for {TARGET_COMPETITION}.')
    episodes_df.to_csv(EPISODES_PATH + "Filtered_episodes.csv")
    
else:
    raise MemoryError #TODO LOAD IN CHUNKS TOO


print("Will load episode agents...")
if can_be_loaded(EPISODE_AGENTS_PATH) is True:#If the size of the csv is small we load it without breaking it into chunks
    episode_agents_df = pd.read_csv(EPISODE_AGENTS_PATH)
    max_score_epagents_df = filter_AgentEps(episode_agents_df, episodes_df, conf_json)
    merged_dataframes = pd.merge(left=episodes_df, right=max_score_epagents_df, left_on='Id', right_on='EpisodeId')
    subid_and_score_pairs = pd.Series(merged_dataframes.UpdatedScore.values,index=merged_dataframes.SubmissionId).to_dict()
    print(f'{len(merged_dataframes)} submissions with score over {conf_json["THRESHOLDS"]["MIN_MATCH_SCORE"]}')
    # Get episodes for these submissions
    subid_epid_pairs = collections.defaultdict(list)
    candidates = get_candidate_eps(subid_and_score_pairs, subid_epid_pairs=subid_epid_pairs, episode_agents_df=episode_agents_df)
    already_scraped, remaining = check_for_new_eps(candidates)
    start_scraping(already_scraped, remaining)

print("Will load episode agents...")