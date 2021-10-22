import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
import datetime
import time, requests, json, os, sys
def filter_AgentEps(episode_agents:DataFrame, episodes:DataFrame, config:dict):
    #filter the dataframes
    episode_agents = episode_agents[episode_agents.EpisodeId.isin(episodes.index)]
    print(f'EpisodeAgents Chunk: {len(episode_agents)} rows after filtering for {config["COMPETITIONS_INFO"]["TARGET_COMP"]}.')
    episode_agents.fillna(0, inplace=True)
    episode_agents = episode_agents.sort_values(by=['Id'], ascending=False)
    # Get top scoring submissions#
    max_df = (episode_agents.sort_values(by=['EpisodeId'], ascending=False).groupby('SubmissionId').head(1).drop_duplicates().reset_index(drop=True))
    max_df = max_df[max_df.UpdatedScore>=config["THRESHOLDS"]["MIN_MATCH_SCORE"]]
    print(f'EpisodeAgents Chunk: {len(episode_agents)} rows with score over {config["THRESHOLDS"]["MIN_MATCH_SCORE"]} .')
    return max_df


def get_candidate_eps(subid_and_score_pairs:dict, subid_epid_pairs:dict, episode_agents_df:DataFrame):
    for key, value in sorted(subid_and_score_pairs.items(), key=lambda kv: kv[1], reverse=True):
        
        #find all the episodes for a particular key ==> subval and store them
        eps = sorted(episode_agents_df[episode_agents_df['SubmissionId'].isin([key])]['EpisodeId'].values,reverse=True)
        subid_epid_pairs[key] = eps
    candidates = len(set([item for sublist in subid_epid_pairs.values() for item in sublist]))
    print(f'{candidates} episodes for these {len(subid_and_score_pairs)} submissions')
    return candidates


def create_info_json(epid:int, epagents_df:DataFrame, episodes_df:DataFrame, json_config):
    ct = episodes_df[episodes_df.index == epid]['CreateTime'].values[0]
    create_seconds = int(time.mktime(datetime.datetime.strptime(ct, "%Y/%m/%d %H:%M:%S").timetuple()))
    es = episodes_df[episodes_df.index == epid]['EndTime'].values[0]
    end_seconds = int(time.mktime(datetime.datetime.strptime(es, "%Y/%m/%d %H:%M:%S").timetuple()))

    agents = []
    for index, row in epagents_df[epagents_df['EpisodeId'] == epid].sort_values(by=['Index']).iterrows():
        print(epagents_df.columns)
        agent = {
            "id": int(row["Id"]),
            "state": int(row["State"]),
            "submissionId": int(row['SubmissionId']),
            "reward": int(row['Reward']),
            "index": int(row['Index']),
            "initialScore": float(row['InitialScore']),
            "initialConfidence": float(row['InitialConfidence']),
            "updatedScore": float(row['UpdatedScore']),
            "updatedConfidence": float(row['UpdatedConfidence']),
            "teamId": int(0)
        }
        
        agents.append(agent)
    current_competition = json_config["Competitions_Info"]["TARGET_COMP"]
    info = {
        "id": int(epid),
        "competitionId": int(json_config["Competitions_Info"]["COMP_IDS"][current_competition]),
        "createTime": {
            "seconds": int(create_seconds)
        },
        "endTime": {
            "seconds": int(end_seconds)
        },
        "agents": agents
    }

    return info



def saveEpisode(epid, episode_agents_df, conf, episodes_df):
    # request
    re = requests.post(conf["URL"], json = {"EpisodeId": int(epid)})
    match_replay_path = os.path.join(conf["DIRECTORIES"]["MATCHES"],'{}.json'.format(epid))
    match_info_path = os.path.join(conf["DIRECTORIES"]["MATCHES"],'{}_info.json'.format(epid))
    # save replay
    with open(match_replay_path, 'w') as f:
        f.write(re.json()['result']['replay'])

    # save match info
    info = create_info_json(epid, episode_agents_df, episodes_df, conf)
    with open(match_info_path, 'w') as f:
        json.dump(info, f)

def check_for_new_eps(candidates, sub_to_episodes, conf):
    all_files = []
    for root, dirs, files in os.walk(conf["DIRECTORIES"]["MATCHES"], topdown=False):
        print(root, dirs, files)
        
        all_files.extend(files)
    
    already_scraped = [int(f.split('.')[0]) for f in all_files 
                        if '.' in f and f.split('.')[0].isdigit() and f.split('.')[1] == 'json']
    remaining = np.setdiff1d([item for sublist in sub_to_episodes.values() for item in sublist],already_scraped)
    print(f'{len(remaining)} of these {candidates} episodes not yet saved')
    print('Total of {} games in existing library'.format(len(already_scraped)))
    return already_scraped, remaining


def start_scraping(episodes, episode_pagents_df, seen, remaining, sub_to_score_top, sub_to_episodes, conf):
    print("Will request available data for the current chunk...")
    total_calls = 0

    start_time = datetime.datetime.now()
    se=0
    for key, value in sorted(sub_to_score_top.items(), key=lambda kv: kv[1], reverse=True):
        if total_calls<=conf["THRESHOLDS"]["MAX_REQUESTS"]:
            print('')
            remaining = sorted(np.setdiff1d(sub_to_episodes[key],seen), reverse=True)
            print(f'submission={key}, LB={"{:.0f}".format(value)}, matches={len(set(sub_to_episodes[key]))}, still to save={len(remaining)}')
            
            for epid in remaining:
                if epid not in seen and total_calls<=conf["THRESHOLDS"]["MAX_REQUESTS"]:
                   
                    saveEpisode(epid, episode_pagents_df, conf, episodes) 
                    
                    se+=1
                    try:
                        size = os.path.getsize(conf["DIRECTORIES"]["MATCHES"]+'{}.json'.format(epid)) / 1e6
                        print(str(total_calls) + f': saved episode #{epid}')
                        seen.append(epid)
                        total_calls+=1
                    except:
                        print('  file {}.json did not seem to save'.format(epid))    
                    time.sleep(conf["THRESHOLDS"]["REQUEST_INTERVAL"])
                if total_calls>(min(3600,conf["THRESHOLDS"]["MAX_REQUESTS"])):
                    break
    
    print(f'\nEpisodes saved: {se}')