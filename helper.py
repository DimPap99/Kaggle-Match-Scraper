import os, psutil
from pandas.core.frame import DataFrame
import pandas as pd
import sys

GB_1 = 10**9 # bytes around 0.93 gb
def df_row_size(file_path) -> int:
    df = pd.read_csv(file_path, nrows=1)
    mem_used = df.memory_usage().sum() #include the index to get fewer rows per chunk just to be safe
    return mem_used


def can_be_loaded(file) -> bool:
    current_mem_avbl = psutil.virtual_memory().available
    file_sz = os.path.getsize(file)
    if file_sz + GB_1 < current_mem_avbl:
        return True
    else:
        return False
def find_mem_chunk_sz(file) -> int:
    file_sz = os.path.getsize(file)
    current_mem_avbl = psutil.virtual_memory().available
    if current_mem_avbl < GB_1:
        raise MemoryError
    else:
        chunk_size = file_sz // (current_mem_avbl - GB_1)
        print(f"Will break the dataframe in chunks of {chunk_size} bytes...")
        return chunk_size

def find_df_chunk_sz(file, with_curr_mem=False) -> int:
    global GB_1
    file_sz = os.path.getsize(file)
    if with_curr_mem is True:
        mem_avbl = psutil.virtual_memory().available
    mem_avbl = 3 * GB_1
    row_sz = df_row_size(file)
    
    return mem_avbl // row_sz



def filter_episodes(original_path:str, new_path:str, config_file:dict) -> None:
    target_competition_str = config_file["COMPETITIONS_INFO"]["TARGET_COMP"]
    # Load episodes and filter them
    episodes_df = load_episodes(original_path)
    print(f"Will filter {original_path} based on the CompetitionId...")
    print(f"{original_path}: {len(episodes_df)} rows before filtering.")
    episodes_df = episodes_df[episodes_df.CompetitionId == config_file["COMPETITIONS_INFO"]["COMP_IDS"][target_competition_str]] 
    print(f"Filtering finished...\n{original_path}: {len(episodes_df)} rows after filtering for {target_competition_str}.")
    print(f"Reseting the index on the Id column...")
    episodes_df = episodes_df.set_index(['Id'])
    episodes_df.to_csv(new_path)
    print(f"Filtered episodes csv is at: {new_path}")
        
def load_episodes(episode_path) -> DataFrame:
    print(f"Will load {episode_path} ...")
    try:
        if can_be_loaded(episode_path) is True:
            return pd.read_csv(episode_path)
        else:
            raise MemoryError #TODO LOAD AND FILTER IN CHUNKS TOO
    except MemoryError:
        print(f"The {episode_path} file's size is too big...")
        sys.exit(1)
    except Exception as err:
        print(f"An exception occured: {str(err)}")
        sys.exit(1)



