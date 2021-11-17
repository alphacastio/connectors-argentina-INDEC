#!/usr/bin/env python
# coding: utf-8

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("ALPHACAST_API_KEY")
alphacast = Alphacast(API_KEY)

#Url base del sitio
url = 'https://www.indec.gob.ar/Nivel4/Tema/3/3/42'
html_page = requests.get(url).text
soup = BeautifulSoup(html_page, 'html.parser')
links = soup.find_all('a', href=True)


#Obtengo la url del archivo del ISAC
xls_links = []
keyword = 'Indicador sintético de la actividad de la construcción, insumos para la construcción'
for link in links:
    if keyword in link.get_text():
        xls_links.append(link.get('href'))

#Mantengo solo el primer elemento de la lista
xls_links = 'https://www.indec.gob.ar' + xls_links[0]


# Levanto la data y armo el dataframe
df1 = pd.read_excel(xls_links, sheet_name = 'Cuadro 1', skiprows=2, header=[0,1,2])

#Elimino las columnas con NaNs
df1.dropna(axis=1, how='all', inplace=True)
df1.dropna(axis=0, how='all', inplace=True)

#Se eliminan las filas que no tengan datos en la tercer columna
df1.dropna(subset=[df1.columns[2]], how='all', inplace=True)

#Completo de la fila 1 en adelante
df1.iloc[1:, 0].fillna(method='ffill', inplace=True)

# Realizo un replacement de los meses
dict_meses = {"Enero": '01', "Febrero": '02', "Marzo": '03', "Abril": '04', "Mayo": '05', "Junio": '06', 
              "Julio": '07', "Agosto": '08', "Septiembre": '09', "Octubre": '10', "Noviembre": '11', "Diciembre": '12'}
df1.iloc[:, 1].replace(dict_meses, inplace=True)

#Unifico los nombres de las columnas
df1.columns = df1.columns.map(' - '.join)
df1 = df1.dropna(how='any', axis=0)

#Se cambian los nombres de las variables
df1 = df1.rename(columns={'Serie original -     ISAC nivel general - Unnamed: 2_level_2': 'ISAC nivel general',
                          'Serie original - Variación porcentual - respecto al mismo mes del año anterior': 'Serie original - Variación porcentual - respecto al mismo mes del año anterior - %',
                          'Serie original - Variación porcentual - acumulada del año respecto a igual acumulado del año anterior':'Serie original - Variación porcentual - acumulada del año respecto a igual acumulado del año anterior - %',
                          'Serie desestacionalizada (1) -     ISAC     nivel general - Unnamed: 6_level_2':'ISAC - sa_orig',
                          'Serie desestacionalizada (1) - Variación porcentual respecto al mes anterior - Unnamed: 7_level_2':'Serie desestacionalizada - Variación porcentual respecto al mes anterior - %',
                          'Serie tendencia-ciclo -     ISAC     nivel general - Unnamed: 9_level_2':'ISAC tendencia-ciclo',
                          'Serie tendencia-ciclo - Variación porcentual respecto al mes anterior - Unnamed: 10_level_2':'Serie tendencia-ciclo - Variación porcentual respecto al mes anterior - %'
                         })


#Creamos la columna fecha y le cambiamos el formato
df1['Date'] = df1[df1.columns[0]].astype(str) + '-' + df1[df1.columns[1]].astype(int).astype(str)
df1['Date'] = pd.to_datetime(df1['Date'], format='%Y-%m')
df1.set_index('Date', inplace=True)
df1 = df1.iloc[:, 2:]

#Levanto la hoja con el ISAC desagregado y creo un segundo dataframe
df2 = pd.read_excel(xls_links, sheet_name = 'Cuadro 2.1', skiprows=2)

#Elimino las filas con NaNs
df2.dropna(subset=[df2.columns[1]], inplace=True)

#Completo las filas de 'Período' con el dato del año
df2.iloc[:, 0].fillna(method='ffill', inplace=True)

#Reemplazo los meses con el diccionario
df2.iloc[:, 1].replace(dict_meses, inplace=True)

#Creo la columna de fecha, le cambio el formato y la seteo como índice
df2['Date'] = df2[df2.columns[0]].astype(str) + '-' + df2[df2.columns[1]].astype(int).astype(str)
df2['Date'] = pd.to_datetime(df2['Date'], format='%Y-%m')
df2.set_index('Date', inplace=True)
df2 = df2.iloc[:, 2:]

#Uno ambos dataframes en uno solo
df = df1.merge(df2, left_index=True, right_index=True, how='outer')


#Realizo limpieza de ciertos strings y agrego la columna country para hacer el upload
df.replace('///', np.nan, inplace=True)
df.columns = df.columns.str.replace('\s+\-\s+\-\s+', ' - ', regex=True)
df.columns = df.columns.str.replace('\s+\(1\)', '', regex=True)



#Cargo la data a Alphacast
df['country'] = 'Argentina'
alphacast.datasets.dataset(15).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)






