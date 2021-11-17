#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import requests
import io

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

months = ['12', '11', '10', '09', '08', '07', '06', '05', '04', '03', '02', '01']
for month in months:
    print('Trying month number:', month)
    url = 'https://www.indec.gob.ar//ftp/cuadros/economia/sh_oferta_demanda_{}_21.xls'.format(month)
    try:
        sheetnames = ['cuadro 1', 'cuadro 8']
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
            # df.Date.values
            #df['Date']=pd.to_datetime(df['Date'])
            df['Date'] = pd.to_datetime(df['Date'], errors="coerce")
            df = df.set_index('Date')
            newCols=[]
            for col in df.columns:
                if col != 'Date':
                    if sheet == 'cuadro 1':
                        colname = "[P2004]-" + col
                        newCols += [colname]
                    elif sheet == 'cuadro 8':
                        colname = "[CP]-" + col
                        newCols += [colname]
            df.columns = newCols    
            if sheet=='cuadro 1':
                dfParcial = df
            else:
                dfParcial = dfParcial.merge(df, how='left', left_index=True, right_index=True)
        print('We have a match in month number', month)
        break
    except:
        print('There are no updated data for month', month)
        pass


dfParcial['[P2004]-Objetos valiosos'] = dfParcial['[P2004]-Objetos valiosos'].fillna(0)
dfParcial['[P2004]-Discrepancia estadística (4)'] = dfParcial['[P2004]-Discrepancia estadística (4)'].fillna(0)
dfParcial['[P2004]-Discrepancia estadística (4)'] = dfParcial['[P2004]-Discrepancia estadística (4)'].str.replace('///','0')


months = ['12', '11', '10', '09', '08', '07', '06', '05', '04', '03', '02', '01']
for month in months:
    print('Trying month number:', month)

    url = 'https://www.indec.gob.ar//ftp/cuadros/economia/sh_oferta_demanda_desest_{}_21.xls'.format(month)
    r = requests.get(url, allow_redirects=False, verify=False)
    try:
        with io.BytesIO(r.content) as dframe:
            df = pd.read_excel(dframe, sheet_name='desestacionalizado n', skiprows=3)

        df = df.rename(columns = {'Año': 'Date'})
        df['Date'] = df['Date'].fillna(method='ffill')

        df = df.rename(columns = {df.columns[1]: 'quarter'})
        df = df.dropna(axis=1, how='all')
        df = df.dropna(axis=0, how='any', subset= df.columns[1:-1])

        def replaceQuarter(x):
            x= str(x)
            x= x.lower()
            if x == 'i':
                x='01-01'
            elif x == 'ii':
                x='04-01'
            elif x == 'iii':
                x='07-01'
            elif x == 'iv':
                x='10-01'
            elif x == 'Total':
                x=np.nan
            return x

        df['quarter'] = df['quarter'].apply(lambda x: replaceQuarter(x))
        df['Date'] = df['Date'].astype(str) + '-' + df['quarter'].astype(str)
        df.Date.values
        del df['quarter']
        #df['Date'] = df['Date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
        df['Date'] = pd.to_datetime(df['Date'], errors="coerce")
        df = df.set_index('Date')
        print('We have a match in month number', month)
        break
    except:
        print('There are no updated data for month', month)
        pass


dfFinal = df.merge(dfParcial, how='outer', left_index=True, right_index=True)
dfFinal = dfFinal[dfFinal.index.notnull()]

dfFinal['country'] = 'Argentina'
alphacast.datasets.dataset(148).upload_data_from_df(dfFinal, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

