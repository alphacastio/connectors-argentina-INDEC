#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

url= 'https://www.indec.gob.ar/ftp/cuadros/economia/icc_variaciones_2016.xls'
df = pd.read_excel(url, skiprows=4, sheet_name='Nivel general y capítulos')
df = df.rename(columns = {df.columns[0]: 'Nivel general y capítulos'})
maxindex = df[df[df.columns[0]] == '* Dato provisorio.'].index[0]

df = df[0:maxindex]
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
    x= x.replace('*', '')
    if x == 'Enero':
        x='01-01'
    elif x == 'Febrero':
        x='02-01'
    elif x == 'Marzo':
        x='03-01'
    elif x == 'Abril':
        x='04-01'
    elif x == 'Mayo':
        x='05-01'
    elif x == 'Junio':
        x='06-01'
    elif x == 'Julio':
        x='07-01'
    elif x == 'Agosto':
        x='08-01'
    elif x == 'Septiembre':
        x='09-01'
    elif x == 'Octubre':
        x='10-01'
    elif x == 'Noviembre':
        x='11-01'
    elif x == 'Diciembre':
        x='12-01'
    return x

df['quarter'] = df['quarter'].apply(lambda x: replaceQuarter(x))
df = df.dropna(how='all', subset= df.columns[2:-1])

    
df = df.dropna(how='any', subset=['quarter'])
df['Date'] = df['Date'].apply(lambda x: x.replace(' ', ''))
df['Date'] = df['Date'] + '-' + df['quarter'].astype(str)
df['Date'] = df['Date'].apply(lambda x: x.replace('.0', ''))
del df['quarter']

df = df.set_index('Date')
for col in df.columns:
    df[col] = df[col].astype(str)
    df = df.replace('-0,0', '0')
    df[col] = df[col].astype(float)

df.columns = ['Nivel general', 'Materiales', 'Mano de obra', 'Gastos generales']

for col in df.columns:
    for i in range(0,len(df[col])):
        if i==0:
            df[col][i] = 100*(1+(df[col][i])/100)
        else:
            prevIndex = i-1
            actualvalue= df[col][i]
            prevValue= df[col][prevIndex]
            ratio = 1+(float(actualvalue)/100)
            newVal = prevValue*ratio
            df[col][i] = newVal

alphacast.datasets.dataset(166).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)




