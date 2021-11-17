#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import datetime
import numpy as np

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


months = ['12', '11', '10', '09', '08', '07', '06', '05', '04', '03', '02', '01']
for month in months:
    print('Trying month number:', month)
    url = "https://www.indec.gob.ar/ftp/cuadros/sociedad/variaciones_salarios_{}_21.xls".format(month)
    try:
        df = pd.read_excel(url, sheet_name='Cuadro 1', skiprows=2)
        df= df.rename(columns={'Unnamed: 1':'Mes'})

        df = df.dropna(how='all', subset=df.columns[1:])

        df.iloc[0,:] = df.iloc[0,:].fillna(method='ffill', axis=0)
        df['Período'] = df['Período'].fillna(method='ffill')

        def replaceDate(x):
            if x=='Enero':
              x='01-01'
            elif x=='Febrero':
                x='02-01'
            elif x=='Marzo':
                x='03-01'
            elif x=='Abril':
                x='04-01'
            elif x=='Mayo':
                x='05-01'
            elif x=='Junio':
                x='06-01'
            elif x=='Julio':
                x='07-01'
            elif x=='Agosto':
                x='08-01'
            elif x=='Septiembre':
                x='09-01'
            elif x=='Octubre':
                x='10-01'
            elif x=='Noviembre':
                x='11-01'
            elif x=='Diciembre':
                x='12-01'
            elif x=='Enero ':
                x='01-01'
            elif x=='Febrero ':
                x='02-01'
            elif x=='Marzo ':
                x='03-01'
            elif x=='Abril ':
                x='04-01'
            elif x=='Mayo ':
                x='05-01'
            elif x=='Junio ':
                x='06-01'
            elif x=='Julio ':
                x='07-01'
            elif x=='Agosto ':
                x='08-01'
            elif x=='Septiembre ':
                x='09-01'
            elif x=='Octubre ':
                x='10-01'
            elif x=='Noviembre ':
                x='11-01'
            elif x=='Diciembre ':
                x='12-01'
            return x

        df['Mes'] = df['Mes'].apply(lambda x: replaceDate(x))
        df['Período'] = df['Período'].apply(lambda x: str(x).replace('*',''))

        df = df.reset_index(drop=True)

        import re

        columns = df[:1].fillna('') # un dataframe con la cantidad de filas que le estoy 
        # pasando
        tempcols=[]
        for col in df.columns:
            if 'Unnamed' in col:
                col = np.nan
            tempcols+=[col] # += es equivalente a 


        df.columns=tempcols
        df.columns = pd.Series(df.columns).fillna(method='ffill', axis=0).tolist()
        columns = columns.replace(np.nan, '')
        temp_cols=[]
        for i in range(0,columns.shape[1]):
            #El primer valor de la columna es el codigo identificador que tiene al worksheet del banco central
            coltemp = [str(df.columns[i])]
            for j in range(0, columns.shape[0]):
                value = str(columns.iloc[j,i])

                if ('Unnamed' in value) | (value=='nan'):
                    value = '' 

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

        #         print(col)
        #    col = prefix[prefijo]+'-'+col
            newColumns += [col]

        df.columns = newColumns
        df = df[1:]


        # Luego eliminamos las columnas que tengan el signo %

        df = df.loc[:, ~(df == '%').any()]


        columns = df[:1].fillna('') # un dataframe con la cantidad de filas que le estoy 
        # pasando
        tempcols=[]
        for col in df.columns:
            if 'Unnamed' in col:
                col = np.nan
            tempcols+=[col] # += es equivalente a 

        df.columns=tempcols
        df.columns = pd.Series(df.columns).fillna(method='ffill', axis=0).tolist()
        columns = columns.replace(np.nan, '')
        temp_cols=[]
        for i in range(0,columns.shape[1]):
            #El primer valor de la columna es el codigo identificador que tiene al worksheet del banco central
            coltemp = [str(df.columns[i])]
            for j in range(0, columns.shape[0]):
                value = str(columns.iloc[j,i])

                if ('Unnamed' in value) | (value=='nan'):
                    value = '' 

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
        df = df[1:]

        df = df.replace( 'nan', np.nan)

        df = df.dropna(how='all')

        df['Período'] = df['Período'].astype(str).apply(lambda x: x.replace(' ', ''))

        df['Date'] = df['Período'] + '-' + df['Mes']

        del df['Período']
        del df['Mes'] 

        from datetime import datetime

        df['Date'] = df['Date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))

        df = df.set_index('Date')

        print('We have a match in month number', month)
        break
    except:
        print('There are no updated data for month', month)
        pass


for col in df.columns:
    df[col] = pd.to_numeric(df[col], errors="coerce")


df['country'] = 'Argentina'
alphacast.datasets.dataset(167).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



