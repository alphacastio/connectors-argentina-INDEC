#!/usr/bin/env python
# coding: utf-8

import requests
import pandas as pd

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("ALPHACAST_API_KEY")
alphacast = Alphacast(API_KEY)


#EMAE Agregado
url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_mensual_base2004.xls"
r_total = requests.get(url, allow_redirects=True)

#EMAE Desagregado
url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_actividad_base2004.xls"
r_sector= requests.get(url, allow_redirects=True)

#####################################
#############EMAE TOTAL#########
#####################################
#Se carga la hoja según la posición en el archivo, no en base a su nombre
df = pd.read_excel(r_total.content, sheet_name=0, skiprows= 2, header=[0])
df.dropna(axis=0, how='all',inplace=True)
df.dropna(axis=1, how='all',inplace=True)
df.dropna(axis = 0, how = 'all', subset=df.columns[1:], inplace=True)


# Date extraction
df[df.columns[0]].fillna(method='ffill', inplace=True)
dict_meses = {"Enero": 1, "Febrero": 2, "Marzo": 3, "Abril": 4, "Mayo": 5, "Junio": 6, "Julio": 7, 
              "Agosto": 8, "Septiembre": 9, "Octubre": 10, "Noviembre": 11, "Diciembre": 12}

df[df.columns[1]].replace(dict_meses, regex=True, inplace=True)
df['Date'] = df[df.columns[0]].astype(str) + '-' + df[df.columns[1]].astype(str)
df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m')
df.drop([df.columns[0], df.columns[1]], axis=1, inplace=True)
df = df.set_index('Date')


#####################################
#############EMAE por sector#########
#####################################
df1 = pd.read_excel(r_sector.content, sheet_name=0, skiprows= 2, header=[0])
df1.dropna(axis=0, how='all',inplace=True)
df1.dropna(axis=1, how='all',inplace=True)
df1.dropna(subset=[df1.columns[1]], inplace=True)

# Date extraction
df1[df1.columns[0]].fillna(method='ffill', inplace=True)
df1[df1.columns[1]].replace(dict_meses, regex=True, inplace=True)
df1['Date'] = df1[df1.columns[0]].astype(str) + '-' + df1[df1.columns[1]].astype(str)
df1['Date'] = pd.to_datetime(df1['Date'], format='%Y-%m')
df1.drop([df1.columns[0], df1.columns[1]], axis=1, inplace=True)
df1 = df1.set_index('Date')

# Fusiono ambos dataframes
df = df.merge(df1, how='outer', left_index=True, right_index=True)

#Convierto los nombres de las columnas a series para hacer el replace con un diccionario
dict_caracteres = {"\n": " ", "á":"a", "é":"e", "í": "i", "ó": "o", "ú": "u"}

df.columns = pd.Series(df.columns).replace(dict_caracteres, regex=True)
df.columns = df.columns.str.strip()

#Agrego la columna de country para hacer el upload
df['country'] = 'Argentina'

# Upload
alphacast.datasets.dataset(14).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)



