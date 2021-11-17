#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np


from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)


sheetname = "Cuadro 23"

#script para buscar el ultimo file distponible. Lo ideal es entrar a la pagina y extraer la url por xpath
for filename in ["cin_{}_".format(y) + str(x) for x in range(2030, 2019, -1) for y in ["IV", "III", "II", "I"]]:
    url = "https://www.indec.gob.ar/ftp/cuadros/economia/{}.xls".format(filename)
    try: 
        df = pd.read_excel(url, sheet_name = sheetname, skiprows=3, header=[0,1])
        break
    except:
        continue


df = df.dropna(how='all').dropna(how='all',axis=1)
df = df.dropna(how='all', subset=df.columns[1:])

if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.map(' - '.join)

for col in df.columns:
    new_col = col.replace('Unnamed: 0_level_0 - Unnamed: 0_level_1', 'Concepto').replace('*','').replace('Año ','').replace(' ','')
    df = df.rename(columns={col: new_col})


df = df.rename(columns={'2020-I':'2021-I','2019-I':'2020-I','2018-I.1':'2019-I'})

df["level_1"] = df["Concepto"]

sublevels = {
    'level_2':['A corto plazo','A largo plazo'],
    'level_3':['Moneda y depósitos','Títulos de deuda','Préstamos ','Créditos y anticipos comerciales','Otros pasivos de deuda',
              'Derechos especiales de giro (asignaciones)','Pasivos de deuda de empresas de inversión directa frente a inversionistas directos',
              'Pasivos de deuda de inversionistas directos frente a empresas de inversión directa','Pasivos de deuda entre empresas emparentadas'],
}

for sublevel in sublevels:
    df[sublevel] = np.nan
    
    df.loc[df["level_1"].isin(sublevels[sublevel]), sublevel] = df["level_1"]
    df.loc[df["level_1"].isin(sublevels[sublevel]), "level_1"] = np.nan 


# In[46]:


for x in range(len(sublevels)+1,0,-1):
    
    df["level_{}_temp".format(x)] = df["level_{}".format(x)].ffill()
    df.loc[df["level_{}".format(x)].notnull(), "level_{}".format(x)] = df["level_{}_temp".format(x)]
    for y in range(1, x):
        df.loc[df["level_{}".format(y)].notnull(), "level_{}_temp".format(x)] = np.nan
        


df["concept"] = df["level_1_temp"]
for y in range (2,len(sublevels)+2):
    df.loc[df["level_{}_temp".format(y)].notnull(), "concept"] = df["concept"] + " - " + df["level_{}_temp".format(y)]
    



df["Concepto"] = df["concept"]
df = df.drop(['level_1','level_2','level_3','level_1_temp','level_2_temp','level_3_temp','concept'],axis=1)

df = df.set_index("Concepto").transpose().reset_index()

def dateWrangler(x):
        x=str(x)
        global y
        list_x = x.split('-')
        if list_x[1] == 'I':
            y = list_x[0]+'-03-01'
        elif list_x[1] == 'II':
            y = list_x[0]+'-06-01'
        elif list_x[1] == 'III':
            y = list_x[0]+'-09-01'
        elif list_x[1] == 'IV':
            y = list_x[0]+'-12-01'
        return y  

df['Date'] =  df['index'].apply(lambda x: dateWrangler(x))
del df["index"]
df = df.set_index('Date')


df["country"] = "Argentina"
alphacast.datasets.dataset(326).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)
