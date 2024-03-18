import os
import requests
import numpy as np
import pandas as pd
import re
DATA_PATH = 'data/MMM_MMM_DAE.csv'

def download_data(url, force_download=False, ):
    # Utility function to donwload data if it is not in disk
    data_path = os.path.join('data', os.path.basename(url.split('?')[0]))
    if not os.path.exists(data_path) or force_download:
        # ensure data dir is created
        os.makedirs('data', exist_ok=True)
        # request data from url
        response = requests.get(url, allow_redirects=True)
        # save file
        with open(data_path, "w") as f:
            # Note the content of the response is in binary form: 
            # it needs to be decoded.
            # The response object also contains info about the encoding format
            # which we use as argument for the decoding
            f.write(response.content.decode(response.apparent_encoding))

    return data_path


def load_formatted_data(data_fname:str) -> pd.DataFrame:
    """ One function to read csv into a dataframe with appropriate types/formats.
        Note: read only pertinent columns, ignore the others.
    """
        column = {
        'nom': str,
        'adr_num': float,
        'adr_voie': str,
        'com_nom':str,
        'com_cp': float,
        'lat_coor1': float,
        'long_coor1': float,
        'freq_mnt': float,
        'dermnt': str,
        'tel': float
    }
    df = pd.read_csv(
        data_fname,
        usecols=column
        )
    df['adresse'] = df['adr_num'].astype(str) + ' ' + df['adr_voie']
    df['com'] = df['com_nom']+ ' ' + df['com_cp'].astype(str) 
    df.drop(columns=['adr_num', 'adr_voie'], inplace=True)
    df.drop(columns=['com_cp', 'com_nom'], inplace=True)
    return df


# once they are all done, call them in the general sanitizing function
def sanitize_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function to do all sanitizing"""
    ...
    return df


# Define a framing function
def frame_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function all framing (column renaming, column merge)"""
    df.rename(...)
    ...
    return df


# once they are all done, call them in the general clean loading function
def load_clean_data(data_path:str=DATA_PATH)-> pd.DataFrame:
    """one function to run it all and return a clean dataframe"""
    df = (load_formatted_data(data_path)
          .pipe(sanitize_data)
          .pipe(frame_data)
    )
    return df


# if the module is called, run the main loading function
if __name__ == '__main__':
    load_clean_data(download_data())

def clean_name(df):
    for index, valeur in enumerate(df['Name']):
        if (valeur==''):
            df.loc[index, 'Name'] = pd.NA
    return df

def clean_freq_mnt(df):
    for index, valeur in enumerate(df['freq_mnt']):
        if valeur == '':
            df.loc[index, 'freq_mnt'] = pd.NA
        if (re.search(r'\d', valeur)):
            df.loc[index, 'freq_mnt'] = pd.NA
    return df



