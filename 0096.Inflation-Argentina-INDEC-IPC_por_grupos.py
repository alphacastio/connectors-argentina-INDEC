#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import pandas as pd
from bs4 import BeautifulSoup

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

#Descargamos la pagina
url = 'https://www.indec.gob.ar/Nivel4/Tema/3/5/31/'

page = requests.get(url)
soup = BeautifulSoup(page.content, 'html.parser')

# Buscamos el link de
# Índices y variaciones porcentuales mensuales e interanuales según divisiones de la canasta, bienes y servicios, 
# clasificación de grupos

link_xls = []
for link in soup.find_all('a'):
    if link.get_text() is not None and 'divisiones de la canasta, bienes y servicios' in link.get_text():
        link_xls.append('https://www.indec.gob.ar' + link.get('href'))

#Se descarga el archivo
xls_file = requests.get(link_xls[0])

df= pd.read_excel(xls_file.content, skiprows=5, sheet_name='Índices IPC Cobertura Nacional')

maxrow = df[df['Total nacional']=='Región GBA'].index[0]
df = df[:maxrow]
df = df.dropna(how='all', subset= df.columns[1:])
df = df.set_index('Total nacional')
df = df.T
df['Date'] = df.index
df = df.set_index('Date')
df['country'] = 'Argentina'

alphacast.datasets.dataset(96).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

