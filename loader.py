import os
import requests
import numpy as np
import pandas as pd

DATA_PATH = 'data/sample_dirty.csv'


def method_com_cp(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Convert com_cp column to string type
    df['com_cp'] = df['com_cp'].astype(str).apply(lambda x: pd.NA if x == '0' else x)
    
    return df



def method_com_nom(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Fill missing values with pd.NA
    df['com_nom'] = df['com_nom'].apply(lambda x: pd.NA if pd.isna(x) or x.strip() == '' else x).str.title()
    
    return df



def method_lat_coor1(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Extract the coordinates from the lat_coor1 column
    df['lat_coor1'] = (
        df['lat_coor1']
        .str.extract(r"(3\.\d+)", expand=False)  # Extract the coordinates
    )
    
    return df


def method_long_coor1(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Extract the coordinates from the long_coor1 column
    df['long_coor1'] = (
        df['long_coor1']
        .str.extract(r"(\d{2}\.\d+)", expand=False)  # Extract the coordinates
    )
    
    return df


def apply_all_methods(df: pd.DataFrame) -> pd.DataFrame:
    df = (df
          .pipe(method_com_cp)
          .pipe(method_com_nom)
          .pipe(method_lat_coor1)
          .pipe(method_long_coor1)
         )
    return df


data_file = 'data/sample_dirty.csv'
df = pd.read_csv(data_file)


result = apply_all_methods(df)

pd.set_option('display.max_rows', None)  
pd.set_option('display.max_columns', None)  
print(result)