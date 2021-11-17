#!/usr/bin/env python
# coding: utf-8

import pandas as pd

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_issp_2021.xls"
df = pd.read_excel(url, sheet_name = "2", skiprows = 2, header = 0)
df = df.iloc[1:len(df.index)-2,0:10]


df = df.replace({"2021*":"2021"})
df = df.replace({"2020*":"2020"})


df["year"] = pd.to_numeric(df[df.columns[0]], errors="coerce").ffill()
df["month"] = df[df.columns[1]]
replace_string = {"Enero": 1, "Febrero": 2, "Marzo": 3, "Abril": 4, "Mayo": 5, "Junio": 6, "Julio": 7, "Agosto": 8, "Septiembre": 9, "Octubre": 10, "Noviembre": 11, "Diciembre": 12, "Enero*": 1, "Febrero*": 2, "Marzo*": 3, "Abril*": 4, "Mayo*": 5, "Junio*": 6, "Julio*": 7, "Agosto*": 8, "Septiembre*": 9, "Octubre*": 10, "Noviembre*": 11, "Diciembre*": 12}
df = df.replace({"month": replace_string})
df["month"] = pd.to_numeric(df["month"], errors="coerce")
df = df[df["month"] <= 12]
df["day"] = 1
df["Date"] = pd.to_datetime(df[["year", "month", "day"]])
del df[df.columns[0]]
del df[df.columns[0]]
del df["day"]
del df["month"]
del df["year"]
df = df.set_index("Date")
df.rename(columns={'ISSP\nNivel general':'Nivel General'}, inplace=True)


df["country"] = "Argentina"


alphacast.datasets.dataset(605).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)




