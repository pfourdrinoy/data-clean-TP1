import pandas as pd
import numpy as np

#com_cp
file_path = r'C:\Users\18509\Desktop\data-cleaning1\data-clean-TP1\data\sample_dirty.csv'


df = pd.read_csv(file_path)

for value in df['com_cp']:
    if value == 0:
        print(pd.NA)
    else:
        print(value)


#long_coor1
for value in df['long_coor1']:
    if pd.isnull(value):  
        print(np.NAN)
    elif str(value).strip() == '':
        print(np.NAN)
    else:
        print(value)

#lat_coor1
for value in df['lat_coor1']:
    if pd.isnull(value): 
        print(np.nan)
    elif str(value).strip() == '' or str(value).strip() == '-':
        print(np.nan)
    else:
        print(value)

#dermnt
for value in df['dermnt']:
    date_value = pd.to_datetime(value, errors='coerce')  
    if date_value == pd.Timestamp('1970-01-01'):  
        print(pd.NaT)
    else:
        print(date_value.date())

#freq_mnt
for value in df['freq_mnt']:
    if value == 0:
        print(pd.NA)
    elif str(value).strip() == '':
        print(pd.NA)
    else:
        print(value)