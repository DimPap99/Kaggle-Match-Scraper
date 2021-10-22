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
from Config_Handler import Config_Handler


script_dir = os.path.dirname(__file__)
config_path = os.path.join(script_dir, "conf.json")
config_handler = Config_Handler(config_path) 


EPISODES_PATH = os.path.join(config_handler.get_directory("KAGGLE_DATA"), "Episodes.csv")
FILTERED_EPISODES_PATH = os.path.join(config_handler.get_directory("KAGGLE_DATA"), "Filtered_episodes.csv")
EPISODE_AGENTS_PATH = os.path.join(config_handler.get_directory("KAGGLE_DATA"), "EpisodeAgents.csv")
MIN_SCORE = config_handler.get_threshold("MIN_MATCH_SCORE")

if config_handler.filter is True:
    filter_episodes(EPISODES_PATH, FILTERED_EPISODES_PATH, config_handler.target_competition_id)


episodes_df = load_episodes(FILTERED_EPISODES_PATH)

print(f"Will load {EPISODE_AGENTS_PATH}...")
print(f"Target competition: {config_handler.target_competition_str}")

if can_be_loaded(EPISODE_AGENTS_PATH) is True:#If the size of the csv is small we load it without breaking it into chunks
    episode_agents_df = pd.read_csv(EPISODE_AGENTS_PATH)
    max_score_epagents_df = filter_AgentEps(episode_agents_df, episodes_df, MIN_SCORE)
    merged_dataframes = pd.merge(left=episodes_df, right=max_score_epagents_df, left_on='Id', right_on='EpisodeId')
    subid_and_score_pairs = pd.Series(merged_dataframes.UpdatedScore.values,index=merged_dataframes.SubmissionId).to_dict()
    print(f'{len(merged_dataframes)} submissions with score over {MIN_SCORE}')

    # Get episodes for these submissions
    subid_epid_pairs = collections.defaultdict(list)

    candidates = get_candidate_eps(subid_and_score_pairs, subid_epid_pairs, episode_agents_df)
    seen_episodes, remaining = check_for_new_eps(candidates, subid_epid_pairs, config_handler.get_directory("MATCHES"))
    start_scraping(seen_episodes, remaining)
else:
    episodes_df = pd.read_csv(os.path.join(config_handler.get_directory("KAGGLE_DATA"),"Filtered_episodes.csv"))
    c_sz = find_df_chunk_sz(EPISODE_AGENTS_PATH)
    
    print(f"Size per dataframe chunk: {c_sz}")
    
    for chunk in pd.read_csv(EPISODE_AGENTS_PATH, chunksize=c_sz):
        max_score_epagents_df = filter_AgentEps(chunk, episodes_df, MIN_SCORE)
        merged_dataframes = pd.merge(left=episodes_df, right=max_score_epagents_df, left_index=True, right_on='EpisodeId') 
        subid_and_score_pairs = pd.Series(merged_dataframes.UpdatedScore.values,index=merged_dataframes.SubmissionId).to_dict()
        print(subid_and_score_pairs)
        print(f'{len(subid_and_score_pairs)} submissions with score over {MIN_SCORE}')
        
        # Get episodes for these submissions
        subid_epid_pairs = collections.defaultdict(list)
        candidates = get_candidate_eps(subid_and_score_pairs, subid_epid_pairs, chunk)

        seen_episodes, remaining = check_for_new_eps(candidates, subid_epid_pairs, config_handler.get_directory("MATCHES"))
        sys.exit(0)
        start_scraping(episodes_df, chunk, seen_episodes, remaining, subid_epid_pairs, sub_to_episodes, conf_json)
        sys.exit(0)

