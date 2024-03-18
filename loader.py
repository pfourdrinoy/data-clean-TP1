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
    return df



# once they are all done, call them in the general sanitizing function
def sanitize_data(df:pd.DataFrame) -> pd.DataFrame:
    df = clean_adr_num(df) 
    df = df.replace(r'\n', ' ', regex=True)
    df = (df
         .pipe(clean_adr_voie)
         .pipe(method_com_cp)
         .pipe(method_com_nom)
         .pipe(method_lat_coor1)
         .pipe(method_long_coor1)
         .pipe(sanitize_dermnt)
         .pipe(clean_freq_mnt)
         .pipe(clean_name)
         .pipe(format_tel1)
         )
    
    return df 


# Define a framing function
def frame_data(df:pd.DataFrame) -> pd.DataFrame:
    """ One function all framing (column renaming, column merge)"""
    df.rename(columns={'nom': 'Nom', 
                   'dermnt':'Date Dernière Maintenance', 
                   'tel1': 'Téléphone', 
                   'freq_mnt':'Fréquence Maintenance', 
                   'lat_coor1': 'Latitude', 
                   'long_coor1':'Longitude'}, inplace=True)

    df['Adresse'] = df['adr_num'].astype(str) + ' ' + df['adr_voie'] + ' ' + df['com_cp'].astype(str)  + ' ' + df['com_nom']
    df.drop(columns=['adr_num', 'adr_voie'], inplace=True)
    df.drop(columns=['com_cp', 'com_nom'], inplace=True)
    return df


# once they are all done, call them in the general clean loading function
def load_clean_data(data_path:str=DATA_PATH)-> pd.DataFrame:
    """one function to run it all and return a clean dataframe"""
    df = (load_formatted_data(data_path)
          .pipe(sanitize_data)
          .pipe(frame_data)
    )
    return df

def clean_name(df):
    for index, valeur in enumerate(df['nom']):
        if (valeur==''):
            df.loc[index, 'nom'] = pd.NA
    return df

def clean_freq_mnt(df):
    for index, valeur in enumerate(df['freq_mnt']):
        if valeur == '':
            df.loc[index, 'freq_mnt'] = pd.NA
        if (re.search(r'\d', valeur)):
            df.loc[index, 'freq_mnt'] = pd.NA
        df['freq_mnt'] = df['freq_mnt'].str.lower()
    return df

def sanitize_dermnt(df:pd.DataFrame) :
    for index, row in df.iterrows():
        if "Tous les ans" in row['dermnt']:
            df.at[index, 'dermnt'] = pd.NaT
        if (re.match(r'^\d{4}-\d{2}-\d{2}$', row['dermnt'])):
            df.at[index, 'dermnt'] = pd.to_datetime(row['dermnt'])
    return df

def method_com_cp(df: pd.DataFrame) -> pd.DataFrame:
    # Convert com_cp column to string type
    df['com_cp'] = df['com_cp'].astype(str).apply(lambda x: pd.NA if x == '0' else x)
    return df

def method_com_nom(df: pd.DataFrame) -> pd.DataFrame:
    # Fill missing values with pd.NA
    df['com_nom'] = df['com_nom'].apply(lambda x: pd.NA if pd.isna(x) or x.strip() == '' else x).str.title()
    return df

def method_lat_coor1(df: pd.DataFrame) -> pd.DataFrame:
    # Extract the coordinates from the lat_coor1 column
    df['lat_coor1'] = (
        df['lat_coor1']
        .str.extract(r"(3\.\d+)", expand=False)  # Extract the coordinates
    )
    return df

def method_long_coor1(df: pd.DataFrame) -> pd.DataFrame:
    # Extract the coordinates from the long_coor1 column
    df['long_coor1'] = (
        df['long_coor1']
        .str.extract(r"(\d{2}\.\d+)", expand=False)  # Extract the coordinates
    )
    return df

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

def clean_adr_voie(df):
    df['adr_voie']=df['adr_voie'].replace('-', pd.NA)
    df['adr_voie']=df['adr_voie'].astype(str).apply(TextClean_adr_voie)
    return df

def TextClean_adr_voie(text):  
    """Garder seulement l'adresse"""
    cleaned_text = re.sub(r'\bMontpellier\b|\bMONTPELLIER\b|[0-9,]', '', text)
    cleaned_text = cleaned_text.rstrip()
    return cleaned_text

def format_tel1(df: pd.DataFrame) -> pd.DataFrame:
    pattern = r"^(?:\+?33|\d{1})?\s*(\d{1})\s*(\d{2})\s*(\d{2})\s*(\d{2})\s*(\d{2})$"
    df['tel1'] = df['tel1'].apply(lambda x: re.sub(pattern, r"+33 \1 \2 \3 \4 \5", x) if x.strip() else pd.NA)
    return df

#if the module is called, run the main loading function
if __name__ == '__main__':
    pd.set_option('display.max_rows', None)  
    pd.set_option('display.max_columns', None) 
    print(load_clean_data()['Téléphone'])