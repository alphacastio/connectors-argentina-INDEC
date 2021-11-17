#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import requests
import re

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

def replaceMonthByNumber(x):
    x=str(x)
    x= x.replace('*','')
    if x == 'Enero':
        x = '-01-01'
    elif x == 'Febrero':
        x = '-02-01'
    elif x == 'Marzo':
        x = '-03-01'
    elif x == 'Abril':
        x = '-04-01'
    elif x == 'Mayo':
        x = '-05-01'
    elif x == 'Junio':
        x = '-06-01'
    elif x == 'Julio':
        x = '-07-01'
    elif x == 'Agosto':
        x = '-08-01'
    elif x == 'Septiembre':
        x = '-09-01'
    elif x == 'Octubre':
        x = '-10-01'
    elif x == 'Noviembre':
        x = '-11-01'
    elif x == 'Diciembre':
        x = '-12-01'
    return x


months = ['12', '11', '10', '09', '08', '07', '06', '05', '04', '03', '02', '01']

for month in months:
    print('Trying month number:', month)
    url= 'https://www.indec.gob.ar/ftp/cuadros/economia/sh_compras_{}_21.xls'.format(month)
    response_data = requests.get(url)
    try:
        html = response_data.content
        df = pd.read_excel(html, sheet_name='CuadroWEB_1', skiprows=2)
        df = df.dropna(how='all')
        df = df.dropna(how='all', axis=1)
        columns = df[:3].fillna('')
        columns = columns.replace(np.nan, '')
        temp_cols=[]
        for i in range(0,columns.shape[1]):
            #El primer valor de la columna es el codigo identificador que tiene al worksheet del banco central
            coltemp = [str(df.columns[i])]
            for j in range(0, columns.shape[0]):
                value = columns.iloc[j,i]
                value = str(value)
                coltemp += [value]
            temp_cols+=['-'.join(coltemp)]

        newColumns = []

        for col in temp_cols:
            col = re.sub(r"(-){2,}", "-", col)

            if len(col) > 0:
                if col[0] == '-':
                    col = col[1:]               
                elif col[-1] == '-':
                    col = col[:-1]

            newColumns += [col]

        df.columns = newColumns
        df = df.rename(columns={df.columns[0]:'Date'})
        df = df.dropna(how='all', subset=df.columns[1:])

        df = df.fillna(method='ffill')
        df = df.dropna(subset=[df.columns[0]])
        df = df.replace('...', np.nan)
        df[df.columns[1]] = df[df.columns[1]].apply(lambda x: replaceMonthByNumber(x)) 
        df['Date'] = df['Date'].astype(str)

        def removeDots(x):
            list_x = x.split('.')
            x= list_x[0]
            return x

        df['Date'] = df['Date'].apply(lambda x: removeDots(x))
        df['Date'] = df['Date'] + df[df.columns[1]]
        df = df.set_index('Date')
        del df[df.columns[0]]
        print('We have a match in month number', month)
        break
    except:
        print('There are no updated data for month', month)
        pass
    
df = df[df.columns[:7]]

df['country'] = 'Argentina'
df.tail()
df = df[df.index.duplicated()==False]
df = df[1:]

alphacast.datasets.dataset(132).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

print("done")