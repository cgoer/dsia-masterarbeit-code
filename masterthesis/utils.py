from pathlib import Path
import math

def get_project_root():
    return Path(__file__).parent

def get_sublists(df, no_threads):
    n = math.ceil(len(df) / no_threads)
    return [df[i:i + n] for i in range(0, df.shape[0], n)]