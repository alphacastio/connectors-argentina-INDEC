#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


## ------------------- ##
## Load Data ##
url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_indicesexpgr_04.xls"
df = pd.read_excel(url, skiprows=2, header=[0,1])
df.columns = df.columns.map(' - '.join)
df = df.dropna(axis=1, how='all')
## ------------------- ##

## ------------------- ##
## Date extraction
df["Período - Unnamed: 0_level_1"] = df["Período - Unnamed: 0_level_1"].str.replace("1º trim." , "-01-01")
df["Período - Unnamed: 0_level_1"] = df["Período - Unnamed: 0_level_1"].str.replace("2º trim." , "-04-01")
df["Período - Unnamed: 0_level_1"] = df["Período - Unnamed: 0_level_1"].str.replace("3º trim." , "-07-01")
df["Período - Unnamed: 0_level_1"] = df["Período - Unnamed: 0_level_1"].str.replace("4º trim." , "-10-01")
df["Período - Unnamed: 0_level_1"] = df["Período - Unnamed: 0_level_1"].str.replace(" " , "")
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
## ------------------- ##

df["country"] = "Argentina"
alphacast.datasets.dataset(283).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
