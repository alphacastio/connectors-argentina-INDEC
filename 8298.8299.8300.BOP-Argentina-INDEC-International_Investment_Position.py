#!/usr/bin/env python
# coding: utf-8

import requests
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


page = requests.get('https://www.indec.gob.ar/Nivel4/Tema/3/35/45')
soup = BeautifulSoup(page.content, 'html.parser')


link_xls=[]
for link in soup.find_all('a'):
    if 'Estadísticas integradas de balanza de pagos, posición de inversión internacional y deuda externa. Años 2006-2021' in link.get_text():
        link_xls.append('https://www.indec.gob.ar' + link.get('href'))

file_xls = requests.get(link_xls[0])


##### Por Categoría de Mercado #####
df = pd.read_excel(file_xls.content, sheet_name='Cuadro 15', skiprows=3)

df = df.dropna(how = 'all', axis = 0).dropna(how = 'all', axis = 1)
df = df.dropna(how = 'all', subset = df.columns[1:])

#Replico el nombre en todas las columnas 'Unnamed'
df.columns = df.columns.astype(str)
df.columns = pd.Series([np.nan if 'Unnamed:' in x else x for x in df.columns.values]).ffill().values.flatten()

# Reemplazo 'Año' y *
df.columns = df.columns.astype(str)
for col in df.columns:
    new_col = col.replace("Año", "").replace('*','')
    df = df.rename(columns={col: new_col})

#Transpongo el df
df.rename(columns={df.columns[0]: "Date"}, inplace=True)
df = df.set_index('Date')
df = df.T
df = df.reset_index()

#Arreglo las fechas de los trimestres y armo 'Date'
df.rename(columns={df.columns[1]: "Trim"}, inplace=True)

df['Trim'] = df['Trim'].replace({
    np.nan:'12-01',
    'I':'03-01',
    'II':'06-01',
    'III':'09-01',
    'IV':'12-01'
})

df['Date'] = df['index'].astype('str')+'-'+df['Trim'].astype('str')
df = df.set_index('Date')
df = df.drop(['index','Trim'], axis = 1)


#Agrego el prefijo 'Activos' y 'Pasivos' a las columnas segun corresponda
#Pasivos
df2 = df.copy()
df2 = df2.iloc[:,25:]
newCols=[]
for col in df2.columns:
    newCols += ['PASIVOS - '+col]        
df2.columns = newCols

#Activos
df = df.iloc[:,:25]
newCols=[]
for col in df.columns:
    newCols += ['ACTIVOS - '+col]        
df.columns = newCols

#Junto de nuevo los dfs y renombro las columnas de totales
df = df.merge(df2, how='outer', left_index=True, right_index=True)

df = df.rename(columns={'ACTIVOS - B90. POSICIÓN DE INVERSIÓN INTERNACIONAL NETA (A-L)':'B90. POSICIÓN DE INVERSIÓN INTERNACIONAL NETA (A-L)',
                       'ACTIVOS - A. ACTIVOS':'A. ACTIVOS',
                       'PASIVOS - L. PASIVOS':'L. PASIVOS'})

df['country'] = 'Argentina'
alphacast.datasets.dataset(8298).upload_data_from_df(df, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)

##### Por Sector Institucional #####
df_inst = pd.read_excel(file_xls.content, sheet_name='Cuadro 16', skiprows=3)

df_inst = df_inst.dropna(how = 'all', axis = 0).dropna(how = 'all', axis = 1)
df_inst = df_inst.dropna(how = 'all', subset = df_inst.columns[1:])

#Replico el nombre en todas las columnas 'Unnamed'
df_inst.columns = df_inst.columns.astype(str)
df_inst.columns = pd.Series([np.nan if 'Unnamed:' in x else x for x in df_inst.columns.values]).ffill().values.flatten()

# Reemplazo 'Año' y *
df_inst.columns = df_inst.columns.astype(str)
for col in df_inst.columns:
    new_col = col.replace("Año", "").replace('*','')
    df_inst = df_inst.rename(columns={col: new_col})

# #Transpongo el df_inst
df_inst.rename(columns={df_inst.columns[0]: "Date"}, inplace=True)
df_inst = df_inst.set_index('Date')
df_inst = df_inst.T
df_inst = df_inst.reset_index()

#Arreglo las fechas de los trimestres y armo 'Date'
df_inst.rename(columns={df_inst.columns[1]: "Trim"}, inplace=True)

df_inst['Trim'] = df_inst['Trim'].replace({
    np.nan:'12-01',
    'I':'03-01',
    'II':'06-01',
    'III':'09-01',
    'IV':'12-01'
})

df_inst['Date'] = df_inst['index'].astype('str')+'-'+df_inst['Trim'].astype('str')

df_inst = df_inst.drop(['index','Trim'], axis = 1)

df_inst.drop(df_inst.index[30], inplace = True)
df_inst = df_inst.set_index('Date')


#Agrego el prefijo 'Activos' y 'Pasivos' a las columnas segun corresponda
#Pasivos
df_inst2 = df_inst.copy()
df_inst2 = df_inst2.iloc[:,20:]

newCols=[]
for col in df_inst2.columns:
    newCols += ['PASIVOS - '+col]        
df_inst2.columns = newCols

#Activos
df_inst = df_inst.iloc[:,:20]
newCols=[]
for col in df_inst.columns:
    newCols += ['ACTIVOS - '+col]        
df_inst.columns = newCols

#Junto de nuevo los dfs y renombro las columnas de totales
df_inst = df_inst.merge(df_inst2, how='outer', left_index=True, right_index=True)

df_inst = df_inst.rename(columns={'ACTIVOS - B90. POSICIÓN DE INVERSIÓN INTERNACIONAL NETA (A-L)':'B90. POSICIÓN DE INVERSIÓN INTERNACIONAL NETA (A-L)',
                                  'ACTIVOS - S121. Banco Central':'S121. Banco Central',
                                  'ACTIVOS - S122. Sociedades captadoras de depósitos':'S122. Sociedades captadoras de depósitos',
                                  'ACTIVOS - S13. Gobierno general':'S13. Gobierno general',
                                  'ACTIVOS - S1Z. Otros sectores':'S1Z. Otros sectores',
                                  'ACTIVOS - A. ACTIVOS':'A. ACTIVOS',
                                  'PASIVOS - L. PASIVOS':'L. PASIVOS'})

df_inst['country'] = 'Argentina'

alphacast.datasets.dataset(8299).upload_data_from_df(df_inst, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)


##### Por categoría funcional e instrumento #####
df_cat = pd.read_excel(file_xls.content, sheet_name='Cuadro 18', skiprows=3)

df_cat = df_cat.dropna(how = 'all', axis = 0).dropna(how = 'all', axis = 1)
df_cat = df_cat.dropna(how = 'all', subset = df_cat.columns[1:])

#Replico el nombre en todas las columnas 'Unnamed'
df_cat.columns = df_cat.columns.astype(str)
df_cat.columns = pd.Series([np.nan if 'Unnamed:' in x else x for x in df_cat.columns.values]).ffill().values.flatten()

# Reemplazo 'Año' y *
df_cat.columns = df_cat.columns.astype(str)
for col in df_cat.columns:
    new_col = col.replace("Año", "").replace('*','')
    df_cat = df_cat.rename(columns={col: new_col})

#Transpongo el df_cat
df_cat.rename(columns={df_cat.columns[0]: "Date"}, inplace=True)
df_cat = df_cat.set_index('Date')
df_cat = df_cat.T
df_cat = df_cat.reset_index()

#Arreglo las fechas de los trimestres y armo 'Date'
df_cat.rename(columns={df_cat.columns[1]: "Trim"}, inplace=True)

df_cat['Trim'] = df_cat['Trim'].replace({
    np.nan:'12-01',
    'I':'03-01',
    'II':'06-01',
    'III':'09-01',
    'IV':'12-01'
})

df_cat['Date'] = df_cat['index'].astype('str')+'-'+df_cat['Trim'].astype('str')
df_cat = df_cat.set_index('Date')
df_cat = df_cat.drop(['index','Trim'], axis = 1)


#Agrego el prefijo 'Activos' y 'Pasivos' a las columnas segun corresponda
#Pasivos
df_cat2 = df_cat.copy()
df_cat2 = df_cat2.iloc[:,13:]

newCols=[]
for col in df_cat2.columns:
    newCols += ['PASIVOS - '+col]        
df_cat2.columns = newCols

# #Activos
df_cat = df_cat.iloc[:,:13]
newCols=[]
for col in df_cat.columns:
    newCols += ['ACTIVOS - '+col]        
df_cat.columns = newCols

#Junto de nuevo los dfs y renombro las columnas de totales
df_cat = df_cat.merge(df_cat2, how='outer', left_index=True, right_index=True)

df_cat = df_cat.rename(columns={'ACTIVOS - Posición neta de otros sectores (A-L)':'Posición neta de otros sectores (A-L)',
                                  'ACTIVOS - A. Activos':'A. Activos',
                                  'PASIVOS - L. Pasivos':'L. Pasivos'})


df_cat['country'] = 'Argentina'
alphacast.datasets.dataset(8300).upload_data_from_df(df_cat, 
                 deleteMissingFromDB = False, onConflictUpdateDB = True, uploadIndex=True)