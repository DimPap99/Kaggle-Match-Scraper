import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
import datetime
import time, requests, json, os
from Config_Handler import Config_Handler
def filter_AgentEps(episode_agents:DataFrame, episodes:DataFrame, score_threshold):
    #filter the dataframes
    episode_agents = episode_agents[episode_agents.EpisodeId.isin(episodes.index)]
    print(f'Current EpisodeAgents Chunk has: {len(episode_agents)} rows after filtering for the target conpetition.')
    episode_agents.fillna(0, inplace=True)
    episode_agents = episode_agents.sort_values(by=['Id'], ascending=False)
    # Get top scoring submissions#
    max_episode_agents = (episode_agents.sort_values(by=['EpisodeId'], ascending=False).groupby('SubmissionId').head(1).drop_duplicates().reset_index(drop=True))
    max_episode_agents = max_episode_agents[max_episode_agents.UpdatedScore>=score_threshold]
    print(f'Current EpisodeAgents Chunk has: {len(max_episode_agents)} rows with score over {score_threshold} .')
    return max_episode_agents


def get_candidate_eps(subid_and_score_pairs:dict, subid_epid_pairs:dict, episode_agents_df:DataFrame):
    for key, value in sorted(subid_and_score_pairs.items(), key=lambda kv: kv[1], reverse=True):
        
        #find all the episodes for a particular key ==> subval and store them
        eps = sorted(episode_agents_df[episode_agents_df['SubmissionId'].isin([key])]['EpisodeId'].values,reverse=True)
        subid_epid_pairs[key] = eps
    candidates = len(set([item for sublist in subid_epid_pairs.values() for item in sublist]))
    print(f'{candidates} episodes for these {len(subid_and_score_pairs)} submissions')
    return candidates


def create_info_json(epid:int, epagents_df:DataFrame, episodes_df:DataFrame, config_handler:Config_Handler):
    ct = episodes_df[episodes_df.index == epid]['CreateTime'].values[0]
    create_seconds = int(time.mktime(datetime.datetime.strptime(ct, "%Y-%m-%d %H:%M:%S").timetuple()))
    es = episodes_df[episodes_df.index == epid]['EndTime'].values[0]
    end_seconds = int(time.mktime(datetime.datetime.strptime(es, "%Y-%m-%d %H:%M:%S").timetuple()))

    agents = []
    for index, row in epagents_df[epagents_df['EpisodeId'] == epid].sort_values(by=['Index']).iterrows():
        
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
    
    info = {
        "id": int(epid),
        "competitionId": config_handler.target_competition_id,
        "createTime": {
            "seconds": int(create_seconds)
        },
        "endTime": {
            "seconds": int(end_seconds)
        },
        "agents": agents
    }

    return info



def saveEpisode(epid, episode_agents_df, config_handler:Config_Handler, episodes_df):
    # request
    re = requests.post(config_handler.url, json = {"EpisodeId": int(epid)})
    
    
    match_replay_path = os.path.join(config_handler.get_directory("MATCHES"),'{}.json'.format(epid))
    match_info_path = os.path.join(config_handler.get_directory("MATCHES"),'{}_info.json'.format(epid))
    # save replay
    with open(match_replay_path, 'w') as f:
        f.write(re.json()['result']['replay'])

    
    info = create_info_json(epid, episode_agents_df, episodes_df, config_handler)
    with open(match_info_path, 'w') as f:
        json.dump(info, f)

def check_for_new_eps(candidates, subid_epid_pairs, matches_dir:str):
    all_files = []
    for root, dirs, files in os.walk(matches_dir, topdown=False):
        all_files.extend(files)
    
    existing_eps = [int(f.split('.')[0]) for f in all_files 
                        if '.' in f and f.split('.')[0].isdigit() and f.split('.')[1] == 'json']
    remaining = np.setdiff1d([item for sublist in subid_epid_pairs.values() for item in sublist],existing_eps)
    
    print(f'{len(remaining)} of these {candidates} episodes not yet saved')
    print('Total of {} games in existing library'.format(len(existing_eps)))
    return existing_eps, remaining

    


def start_scraping(episodes, episode_pagents_df, seen, remaining, sub_to_score_top, sub_to_episodes, config_handler:Config_Handler):
    print("Will request available data for the current chunk...")
    total_calls = 0
    saved_eps=0
    for key, value in sorted(sub_to_score_top.items(), key=lambda kv: kv[1], reverse=True):
        if total_calls<=config_handler.get_threshold("MAX_REQUESTS"):
            print('')
            remaining = sorted(np.setdiff1d(sub_to_episodes[key],seen), reverse=True)
            print(f'submission={key}, LB={"{:.0f}".format(value)}, matches={len(set(sub_to_episodes[key]))}, still to save={len(remaining)}')
            for epid in remaining:
                if epid not in seen and total_calls<=config_handler.get_threshold("MAX_REQUESTS"):
                   
                    saveEpisode(epid, episode_pagents_df, config_handler, episodes) 
                    
                    
                    saved_file_path = os.path.join(config_handler.get_directory("MATCHES"), '{}.json'.format(epid))
                    if os.path.exists(saved_file_path):
                        print(f"Request {str(total_calls)} : saved episode #{epid}")
                        seen.append(epid)
                        total_calls+=1
                        saved_eps+=1
                    else:
                        print(f'The file {epid}.json was not saved')
                        raise FileNotFoundError   
                     
                    time.sleep(config_handler.get_threshold("REQUEST_INTERVAL"))
                if total_calls> config_handler.get_threshold("MAX_REQUESTS"):
                    print("Total amount of daily requests has been surpassed...")
                    break
    
    print(f'\nEpisodes saved: {saved_eps}')