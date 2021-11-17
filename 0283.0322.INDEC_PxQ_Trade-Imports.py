#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_indicesexpgr_04.xls"
df = pd.read_excel(url, skiprows=2, header=[0,1])
df.columns = df.columns.map(' - '.join)
df = df.dropna(axis=1, how='all')
df = df.dropna(how='all', subset = df.columns[1:])

df["Período - Unnamed: 0_level_1"] = df["Período - Unnamed: 0_level_1"].str.replace("1º trim." , "-01-01")
df["Período - Unnamed: 0_level_1"] = df["Período - Unnamed: 0_level_1"].str.replace("2º trim." , "-04-01")
df["Período - Unnamed: 0_level_1"] = df["Período - Unnamed: 0_level_1"].str.replace("3º trim." , "-07-01")
df["Período - Unnamed: 0_level_1"] = df["Período - Unnamed: 0_level_1"].str.replace("4º trim." , "-10-01")
df["Período - Unnamed: 0_level_1"] = df["Período - Unnamed: 0_level_1"].str.replace(" " , "")
df["Período - Unnamed: 0_level_1"] = df["Período - Unnamed: 0_level_1"].str.replace("*" , "")

df["year"] = df["Período - Unnamed: 0_level_1"].str.split("-", expand = True)[0].replace('',np.nan).fillna(method="ffill")
df["month"] = df["Período - Unnamed: 0_level_1"].str.split("-", expand = True)[1]
df["day"] = df["Período - Unnamed: 0_level_1"].str.split("-", expand = True)[2]


df["Date"] = pd.to_datetime(df[["year", "month", "day"]], errors="coerce")
df = df[df["Date"].notnull()]
del df["Período - Unnamed: 0_level_1"]
del df["day"]
del df["month"]
del df["year"]
df = df.set_index("Date")


df["country"] = "Argentina"
alphacast.datasets.dataset(283).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


### BOP-Argentina-INDEC-Indices_of_value,_price_and_quantity_of_imports_by_economic_use

df1 = pd.read_excel('https://www.indec.gob.ar/ftp/cuadros/economia/sh_indicesimpue_04.xls',skiprows = 2, sheet_name = 'sh_indicesimpue_04', header=[0,1])

df1.columns = df1.columns.map(' - '.join)

df1 = df1.dropna(axis=1, how='all')
df1["Período - Unnamed: 0_level_1"] = df1["Período - Unnamed: 0_level_1"].str.replace("1º trim." , "-01-01")
df1["Período - Unnamed: 0_level_1"] = df1["Período - Unnamed: 0_level_1"].str.replace("2º trim." , "-04-01")
df1["Período - Unnamed: 0_level_1"] = df1["Período - Unnamed: 0_level_1"].str.replace("3º trim." , "-07-01")
df1["Período - Unnamed: 0_level_1"] = df1["Período - Unnamed: 0_level_1"].str.replace("4º trim." , "-10-01")
df1["Período - Unnamed: 0_level_1"] = df1["Período - Unnamed: 0_level_1"].str.replace("*" , "")
df1["Período - Unnamed: 0_level_1"] = df1["Período - Unnamed: 0_level_1"].str.replace(" " , "")
df1["year"] = df1["Período - Unnamed: 0_level_1"].str.split("-", expand = True)[0].replace('',np.nan).fillna(method="ffill")
df1["month"] = df1["Período - Unnamed: 0_level_1"].str.split("-", expand = True)[1]
df1["day"] = df1["Período - Unnamed: 0_level_1"].str.split("-", expand = True)[2]
df1["Date"] = pd.to_datetime(df1[["year", "month", "day"]], errors="coerce")
df1 = df1[df1["Date"].notnull()]
del df1["Período - Unnamed: 0_level_1"]
del df1["day"]
del df1["month"]
del df1["year"]
df1 = df1.set_index("Date")
df1["country"] = "Argentina"

alphacast.datasets.dataset(322).upload_data_from_df(df1, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
