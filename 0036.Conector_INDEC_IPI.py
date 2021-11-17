#!/usr/bin/env python
# coding: utf-8

# In[12]:


import pandas as pd
import numpy as np

import requests
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipi_manufacturero_2021.xls"

def replaceString(x):
    x=str(x)
    x = x.replace("Enero", '01').replace("Febrero", '02').replace("Marzo", '03').replace("Abril", '04').replace("Mayo", '05').replace("Junio", '06').replace("Julio", '07').replace("Agosto", '08').replace("Septiembre", '09').replace("Octubre", '10').replace("Noviembre", '11').replace("Diciembre", '12')
    return x

def remove_ast(x):
            x = str(x)
            x = x.replace('*','')
            return x


# In[14]:


#EMAE TOTAL

dfFinal = pd.DataFrame([])
sheetnames = ['Cuadro 2', 'Cuadro 1']
for sheetname in sheetnames:
    if sheetname == 'Cuadro 2':
        df = pd.read_excel(url, 
                                   sheet_name=sheetname, 
                                   skiprows= 3, 
                                   header=[0])
        df.dropna(axis=0, how='all',inplace=True)


        df['Período'] = df['Período'].fillna(method='ffill')

        df = df.rename(columns = {'Período': 'Date', 'Unnamed: 2': 'month'})
        df['Date'] = df['Date'].apply(lambda x: remove_ast(x))

        df['month'] = df['month'].apply(lambda x: replaceString(x))
        df["month"] = pd.to_numeric(df["month"], errors="coerce")
        df = df.dropna(how='all', subset=['month'])
        df["month"] = df["month"].astype(int)


        df = df[df["month"] <= 12]
        df["day"] = 1
        df['Date'] = df['Date']+ '-' + df['month'].astype(str) + '-' + df['day'].astype(str)
        df["Date"] = pd.to_datetime(df['Date'])
        del df["day"]
        del df["month"]

        df = df[df.columns[1:]]

        df.columns = df.columns.str.strip()
        df = df.fillna(np.nan)
        df = df.set_index("Date")
        dfFinal = df
        
    else:
        df = pd.read_excel(url, 
                           sheet_name=sheetname, 
                           skiprows= 3, 
                           header=[0])
        df=df.dropna(axis=1, how='all')    
        df = df[3:]
        df['Período'] = df['Período'].fillna(method='ffill')
        del df[df.columns[0]]
        df = df.dropna(axis=0, how='all', subset= df.columns[1:])
        df = df.rename(columns = {'Período': 'Date', 
                                  'Unnamed: 2': 'month', 
                                  'Serie desestacionalizada (1)': 'Serie desestacionalizada'})
        df.columns = df.columns.str.strip().str.replace("\n", " ")             .str.replace("á", "a")             .str.replace("é", "e")             .str.replace("í", "i")             .str.replace("ó", "o")             .str.replace("ú", "u")
        
        df = df[['Date', 'month', 'Serie desestacionalizada']]
        df['Date'] = df['Date'].apply(lambda x: remove_ast(x))
        df['month'] = df['month'].apply(lambda x: replaceString(x))
        df["day"] = 1
        df['Date'] = df['Date']+ '-' + df['month'].astype(str) + '-' + df['day'].astype(str)
        df["Date"] = pd.to_datetime(df['Date'])
        
        del df["day"]
        del df["month"]
        df = df.set_index("Date")
        dfFinal = dfFinal.merge(df, how='left', left_index= True, right_index=True)
        
dfFinal['country'] = 'Argentina'
dfFinal = dfFinal.rename(columns={'IPI Manufacturero':'IPI','Serie desestacionalizada':'IPI - sa_orig'})

alphacast.datasets.dataset(36).upload_data_from_df(dfFinal, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

