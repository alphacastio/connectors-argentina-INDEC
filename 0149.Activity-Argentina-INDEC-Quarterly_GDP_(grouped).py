#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

url = 'https://www.indec.gob.ar//ftp/cuadros/economia/sh_oferta_demanda_03_21.xls'

sheetnames = ['cuadro 4', 'cuadro 12']
dfParcial = pd.DataFrame([])
for sheet in sheetnames:
    df = pd.read_excel(url, skiprows=3, sheet_name=sheet)
    maxindex = df[df[df.columns[0]] == '(1) Datos provisorios.'].index[0]

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
        if x == '1º trimestre':
            x='01-01'
        elif x == '2º trimestre':
            x='04-01'
        elif x == '3º trimestre':
            x='07-01'
        elif x == '4º trimestre':
            x='10-01'
        elif x == 'Total':
            x=np.nan
        return x

    df['quarter'] = df['quarter'].apply(lambda x: replaceQuarter(x))
    df = df.dropna(how='all', subset= df.columns[2:-1])

    df = df.dropna(how='any', subset=['quarter'])
    df['Date'] = df['Date'].apply(lambda x: x.replace(' ', ''))
    df['Date'] = df['Date'] + '-' + df['quarter'].astype(str)
    df['Date'] = df['Date'].apply(lambda x: x.replace('.0', ''))
    del df['quarter']
    df.reset_index()

    df = df.set_index('Date')
    newCols=[]
    for col in df.columns:
        if col != 'Date':
            if sheet == 'cuadro 4':
                colname = "[P2004]-" + col
                newCols += [colname]
            elif sheet == 'cuadro 12':
                colname = "[CP]-" + col
                newCols += [colname]
    df.columns = newCols

    if sheet=='cuadro 4':
        dfParcial = df
    else:
        dfParcial = dfParcial.merge(df, how='left', left_index=True, right_index=True)
        
dfParcial.index = pd.to_datetime(dfParcial.index, format="%Y-%m-%d")
dfParcial["country"] = "Argentina"


alphacast.datasets.dataset(149).upload_data_from_df(dfParcial, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



