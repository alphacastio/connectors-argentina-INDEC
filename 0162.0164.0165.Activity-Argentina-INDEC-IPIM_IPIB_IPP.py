#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


filename = r'http://www.indec.gob.ar/ftp/cuadros/economia/series_sipm_dic2015.xls'
sheet_names = ['IPIM', 'IPIB', 'IPP']
connectorNumber = [162, 164, 165]
for i in range(0,len(sheet_names)):
    header = [0,1, 2, 3]
    skiprows = 3
    header = [0]
    year= "year" 
    df = pd.read_excel(filename, skiprows=skiprows,header=header
                       , sheet_name=sheet_names[i])
    df = df.T
    del df[1]
    header_row = 1
    df = df.reset_index()
    df.at[1,0]= "month"
    df.at[1,'index']= "year"
    df.columns = df.iloc[header_row]
    df = df.iloc[2:,:]
    df.dropna(axis=0, how='all',inplace=True)
    df.dropna(axis=1, how='all',inplace=True)
    df = df.reset_index()
    del df['index']
    replace_string = {"Ene": 1, "Feb": 2, "Mar": 3, "Abr": 4, "May": 5, "Jun": 6, "Jul": 7, "Ago": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dic": 12, "Set": 9, "Sep.": 9}
    df = df.replace({"month": replace_string})
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df = df[df["month"] <= 12]
    df["day"] = 1

    #df = df.replace({"2021*": "2021","2020*": "2020","2022*": "2022"})
    def remove_ast(x):
        x = str(x)
        x = x.replace('*','')
        return x
    df['year'] = df['year'].apply(lambda x: remove_ast(x))

    df["year"] = pd.to_numeric(df[year], errors="coerce").ffill()
    df["Date"] = pd.to_datetime(df[["year", "month", "day"]])
    del df["day"]
    del df["month"]
    del df["year"]


    df.columns = df.columns.str.strip().str.replace("á", "a")
    df.columns = df.columns.str.strip().str.replace("é", "e")
    df.columns = df.columns.str.strip().str.replace("í", "i")
    df.columns = df.columns.str.strip().str.replace("ó", "o")
    df.columns = df.columns.str.strip().str.replace("ú", "u")

    df.columns = df.columns.str.strip()
    df = df.fillna(np.nan)
    df = df.set_index("Date")
    df["country"] = "Argentina"
    cols=pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique(): 
        cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
    df.columns=cols
    df = df.reset_index()
    df = df.set_index('Date')

    alphacast.datasets.dataset(connectorNumber[i]).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



