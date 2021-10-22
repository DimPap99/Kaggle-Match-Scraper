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
FILTERED_EPISODES_PATH = os.path.join(conf_json["DIRECTORIES"]["KAGGLE_DATA"], "Filtered_episodes.csv")

EPISODE_AGENTS_PATH = os.path.join(conf_json["DIRECTORIES"]["KAGGLE_DATA"], "EpisodeAgents.csv")


if conf_json["FILTER_EPISODES"] is True:
    filter_episodes(EPISODES_PATH, FILTERED_EPISODES_PATH, conf_json)


episodes_df = load_episodes(FILTERED_EPISODES_PATH)

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
else:
    episodes_df = pd.read_csv(os.path.join(conf_json["DIRECTORIES"]["KAGGLE_DATA"],"Filtered_episodes.csv"))
    c_sz = find_df_chunk_sz(EPISODE_AGENTS_PATH)
    print(c_sz)
    print(f"Size per dataframe chunk: {c_sz}")
    
    for chunk in pd.read_csv(EPISODE_AGENTS_PATH, chunksize=c_sz):
        max_df = filter_AgentEps(episode_agents=chunk, episodes=episodes_df, config=conf_json)
        max_df = pd.merge(left=episodes_df, right=max_df, left_index=True, right_on='EpisodeId') 
        subid_epid_pairs = pd.Series(max_df.UpdatedScore.values,index=max_df.SubmissionId).to_dict()
        
        print(subid_epid_pairs)
        print(f'{len(subid_epid_pairs)} submissions with score over {conf_json["THRESHOLDS"]["MIN_MATCH_SCORE"]}')
        
        # Get episodes for these submissions
        sub_to_episodes = collections.defaultdict(list)
        candidates = get_candidate_eps(subid_epid_pairs, subid_epid_pairs=subid_epid_pairs, episode_agents_df=chunk)
        seen_episodes, remaining = check_for_new_eps(candidates, sub_to_episodes, conf_json)
        sys.exit(0)
        start_scraping(episodes_df, chunk, seen_episodes, remaining, subid_epid_pairs, sub_to_episodes, conf_json)
        sys.exit(0)

