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
        'com_cp': float,
        'com_nom':str,
        'tel1': float,
        'freq_mnt': float,
        'dermnt': str,
        'lat_coor1': float,
        'long_coor1': float
    }
    df = pd.read_csv(
        data_fname,
        usecols=column
        )
    # df['adresse'] = df['adr_num'].astype(str) + ' ' + df['adr_voie']
    # df['com'] = df['com_nom']+ ' ' + df['com_cp'].astype(str) 
    # df.drop(columns=['adr_num', 'adr_voie'], inplace=True)
    # df.drop(columns=['com_cp', 'com_nom'], inplace=True)
    return df


# once they are all done, call them in the general sanitizing function
def sanitize_data(df:pd.DataFrame) -> pd.DataFrame:
    print(df['adr_voie'])
    df = clean_adr_num(df)
    df = clean_adr_voie(df)
    print(df['adr_voie'])
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
    print(df)
    return df


# if the module is called, run the main loading function
# if __name__ == '__main__':
#     load_clean_data(download_data())

def clean_adr_voie(df):
    df['adr_voie']=df['adr_voie'].replace('-', pd.NA)
    df['adr_voie']=df['adr_voie'].astype(str).apply(TextClean_adr_voie)
    return df

def TextClean_adr_voie(text):  
    """Garder seulement l'adresse"""
    cleaned_text = re.sub(r'\bMontpellier\b|\bMONTPELLIER\b|[0-9,]', '', text)
    cleaned_text = cleaned_text.rstrip()
    return cleaned_text

def clean_adr_num(df):
    df['adr_num']=df['adr_num'].replace(0, pd.NA)
    df['adr_num']=df['adr_num'].replace('-', pd.NA)
    df['adr_num']=df['adr_num'].astype(str).apply(TextClean_adr_num)
    return df

def TextClean_adr_num(text):
    """Garder seulement le numéro de voierie"""
    if re.search(r'\bbis\b|\bter\b', text):
        cleaned_text = re.sub(r'[^0-9- ]', '', text)
        cleaned_text = cleaned_text + " bis"
    else:
        cleaned_text = re.sub(r'[^0-9-]', '', text)
    return cleaned_text

df=load_formatted_data('data/sample_dirty.csv')
sanitize_data(df)
