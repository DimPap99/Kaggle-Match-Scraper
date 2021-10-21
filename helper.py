import os, psutil
import pandas as pd
def df_row_size(file_path):
    df = pd.read_csv(file_path, nrows=1)
    mem_used = df.memory_usage().sum() #include the index to get fewer rows per chunk just to be safe
    return mem_used

GB_1 = 10**9 # bytes around 0.93 gb
def can_be_loaded(file):
    current_mem_avbl = psutil.virtual_memory().available
    file_sz = os.path.getsize(file)
    if file_sz + GB_1 < current_mem_avbl:
        return True
    else:
        return False
def find_mem_chunk_sz(file):
    file_sz = os.path.getsize(file)
    current_mem_avbl = psutil.virtual_memory().available
    if current_mem_avbl < GB_1:
        raise MemoryError
    else:
        chunk_size = file_sz // (current_mem_avbl - GB_1)
        print(f"Will break the dataframe in chunks of {chunk_size} bytes...")
        return chunk_size

def find_df_chunk_sz(file, with_curr_mem=False):
    global GB_1
    file_sz = os.path.getsize(file)
    if with_curr_mem is True:
        mem_avbl = psutil.virtual_memory().available
    mem_avbl = 3 * GB_1
    row_sz = df_row_size(file)
    
    return mem_avbl // row_sz



