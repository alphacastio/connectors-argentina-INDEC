#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import numpy as np

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

url = 'https://www.indec.gob.ar/ftp/cuadros/sociedad/cuadros_eph_informe_03_21.xls'
sheetnames = 'Cuadro 1.1'

df = pd.read_excel(url, skiprows=3, sheet_name=sheetnames)
df.head()
df = df.dropna(how='all', subset=df.columns[1:])
df = df.set_index(df.columns[0])
df= df.T
df['Date'] = df.index

def replaceWithNan(x):
    x= str(x)
    if "Unnamed:" in x:
        x=np.nan
    return x

def replaceOthers(x):
    x=str(x)
    x=x.replace('Año ', '')
    x=x.replace(" (1)", '')
    x=x.replace(" (2)", '')
    x=x.replace(" (3)", '')
    x=x.replace(" (4)", '')
    x=x.replace("(1)", '')
    x=x.replace("(2)", '')
    x=x.replace("(3)", '')
    x=x.replace("(4)", '')
    return x

df['Date'] = df['Date'].apply(lambda x: replaceWithNan(x))
df['Date'] = df['Date'].fillna(method='ffill')
df['Date'] = df['Date'].apply(lambda x: replaceOthers(x))
df = df.rename(columns = {df.columns[0]: 'quarter'})
df = df.dropna(axis=1, how='all')
df = df.dropna(axis=0, how='all', subset= df.columns[1:-1])



df = df.reset_index()
del df['index']

def replaceQuarter(x):
    x=str(x)
    if (x == '1° trimestre') | (x == '1º trimestre'):
        x='01-01'
    elif (x == '2° trimestre') | (x == '2º trimestre'):
        x='04-01'
    elif (x == '3° trimestre') | (x == '3º trimestre'):
        x='07-01'
    elif (x == '4° trimestre') | (x == '4º trimestre'):
        x='10-01'
    elif x == 'Total':
        x=np.nan
    return x

df['quarter'] = df['quarter'].apply(lambda x: replaceOthers(x))
# df['quarter'] = df['quarter'].apply(lambda x: x.replace(' ', ''))
df['quarter'] = df['quarter'].apply(lambda x: replaceQuarter(x))
df = df.dropna(how='all', subset= df.columns[2:-1])
df = df.dropna(how='any', subset=['quarter'])
df['Date'] = df['Date'].apply(lambda x: x.replace(' ', ''))
df['Date'] = df['Date'] + '-' + df['quarter'].astype(str)
df['Date'] = df['Date'].apply(lambda x: x.replace('.0', ''))
del df['quarter']
df.reset_index()
df = df.set_index('Date')

df['country'] = 'Argentina'
alphacast.datasets.dataset(152).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



# In[ ]:




