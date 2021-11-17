#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests
import numpy as np

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

### TOTAL AGLOMERADOS ###
url = "https://www.economia.gob.ar/download/infoeco/apendice3a.xlsx"
r = requests.get(url, allow_redirects=False, verify=False)
df = pd.read_excel(r.content, skiprows=8, sheet_name='EPH-Poblaciones', header = [0,1])
df = df.dropna(how='all')


df = df.iloc[:,1:9]
#Uno los nombres de las columnas
if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.map('-'.join)


# In[18]:


df = df[1:]
df = df.reset_index(drop=True)
df = df.rename(columns = {'Período-Unnamed: 1_level_1':'Date'})


beginning = df[df['Date'] == '49.2_IT_0_0_13'].index[0]
beginning += 1
df = df[beginning:]

df = df.dropna(how='all', subset=df.columns[1:]).dropna(how='all', axis =1)
df['Date']=df['Date'].replace('I 14','I.14')

def dateWrangler(x):
    x=str(x)
    x= x.replace('*','').replace(' ','')
    list_x = x.split('.')
    if list_x[0] == 'I':
        y = '20'+list_x[1]+'-01-01'
    elif list_x[0] == 'II':
        y = '20'+list_x[1]+'-04-01'
    elif list_x[0] == 'III':
        y = '20'+list_x[1]+'-07-01'
    elif list_x[0] == 'IV':
        y = '20'+list_x[1]+'-10-01'
    else:
        y = np.nan
    return y

df['Date'] = df['Date'].apply(lambda x: dateWrangler(x))
df = df.dropna (how='all', subset=['Date'])
df['Date'] = pd.to_datetime(df['Date'])
df = df.set_index('Date')


### TOTAL URBANO ###
Dict = {'Expansion EPH a TU - 1991-2010': ['461.2_INDICE_TIEMPO_0_T_13_6'], 
        'Expansion EPH a TU - Desde 2010': ['461.3_INDICE_TIEMPO_AEA_T_13_92']
       }

dfUrb = pd.DataFrame([])
for key, value in Dict.items():
    df2 = pd.read_excel(r.content, skiprows=8, sheet_name= key, header = [0,1])
    df2 = df2.dropna(how='all')

    df2 = df2.iloc[:,1:9]

    #Uno los nombres de las columnas
    if isinstance(df2.columns, pd.MultiIndex):
            df2.columns = df2.columns.map('-'.join)

    df2 = df2.reset_index(drop=True)

    df2 = df2.rename(columns = {'Período-Unnamed: 1_level_1':'Date'})

    beginning = df2[df2['Date'] == value[0]].index[0]
    beginning += 1
    df2 = df2[beginning:]

    df2 = df2.dropna(how='any', subset=df2.columns[0:]).dropna(how='all', axis =1)
    df2['Date']= df2['Date'].replace('I 14','I.14')

    def dateWrangler(x):
        x=str(x)
        x= x.replace('*','').replace(' ','')
        list_x = x.split('.')
        if list_x[0] == 'I':
            y = '20'+list_x[1]+'-01-01'
        elif list_x[0] == 'II':
            y = '20'+list_x[1]+'-04-01'
        elif list_x[0] == 'III':
            y = '20'+list_x[1]+'-07-01'
        elif list_x[0] == 'IV':
            y = '20'+list_x[1]+'-10-01'
        else:
            y = np.nan
        return y

    df2['Date'] = df2['Date'].apply(lambda x: dateWrangler(x))
    df2 = df2.dropna (how='all', subset=['Date'])
    df2['Date'] = pd.to_datetime(df2['Date'])
    df2 = df2.set_index('Date')
    
    dfUrb = dfUrb.append([df2])


dfFinal = df.merge(dfUrb, how='outer', left_index=True, right_index=True)
dfFinal['country'] = 'Argentina'
dfFinal = dfFinal.drop_duplicates(subset = 'TOTAL AGLOMERADOS EPH-PEA')
dfFinal = dfFinal.replace('s/d', np.nan)
dfFinal.columns =  dfFinal.columns.str.replace('\n', '').str.replace('2','')

alphacast.datasets.dataset(294).upload_data_from_df(dfFinal, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


