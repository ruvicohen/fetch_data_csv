import os
import pandas as pd


def read_file(file_path):
    try:
        df = pd.read_csv(file_path, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding="latin1", low_memory=False)
    return df

def get_file_path(relative_path):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, relative_path)

def read_csv_file(file_path):
    return read_file(file_path)