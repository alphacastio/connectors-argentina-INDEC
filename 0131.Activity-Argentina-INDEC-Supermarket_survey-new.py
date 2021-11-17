#!/usr/bin/env python
# coding: utf-8

# In[10]:


import pandas as pd
from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

def replaceMonthByNumber(x):
    x = x.str.replace('*','')
    meses = {   'Enero': 1,
                'Febrero':2,
                'Marzo': 3,
                'Abril': 4,
                'Mayo':5,
                'Junio':6,
                'Julio':7,
                'Agosto':8,
                'Septiembre':9,
                'Octubre':10,
                'Noviembre':11,
                'Diciembre':12}
    for mes in meses:
        x = x.replace(mes,meses[mes])
    return x


def fix_data(df, year_col, month_col, join_col=True):
    if join_col:
        df.columns = df.columns.map(' - '.join)    

    df["year"] = pd.to_numeric(df[year_col].ffill(), errors='coerce')
    df["month"] = replaceMonthByNumber(df[month_col])
    df["day"] = 1
    df["Date"] = pd.to_datetime(df[["year", "month", "day"]], errors="coerce")
    df = df[df["Date"].notnull()]
    df = df.set_index("Date")
    del df["year"]
    del df["day"]
    del df["month"]
    del df[year_col]
    del df[month_col]
    return df


df_merge = pd.DataFrame()
df = pd.read_excel('https://www.indec.gob.ar/ftp/cuadros/economia/sh_super_mayoristas.xls', sheet_name="Cuadro 1", skiprows=2, header=[0,1])
df = fix_data(df, "Período - Unnamed: 0_level_1", "Período - Unnamed: 1_level_1")
df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)


df = pd.read_excel('https://www.indec.gob.ar/ftp/cuadros/economia/sh_super_mayoristas.xls', sheet_name="Cuadro 2", skiprows=2, header=[0,1])
df = fix_data(df, "Período - Unnamed: 0_level_1", "Período - Unnamed: 1_level_1")
df = df.dropna(how='all', axis=1)
df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)

df = pd.read_excel('https://www.indec.gob.ar/ftp/cuadros/economia/sh_super_mayoristas.xls', sheet_name="Cuadro 3", skiprows=2, header=[0])
df["Unnamed: 0"] = df["Unnamed: 0"].ffill()
df = df[df["Unnamed: 0"]=="Total"]
del df["Unnamed: 0"]

df = fix_data(df, "Período", "Unnamed: 2", join_col=False)

for col in df.columns:
    df = df.rename(columns={col: "Grupo de Artículos - " + col})
df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)    


df = pd.read_excel('https://www.indec.gob.ar/ftp/cuadros/economia/sh_super_mayoristas.xls', sheet_name="Cuadro 4", skiprows=2, header=[0, 1])
first_col = df.columns[0]
df[first_col] = df[first_col].ffill()
df = df[df[first_col]=="Total"]
del df[first_col]
df = fix_data(df, "Período - Unnamed: 1_level_1", "Período - Unnamed: 2_level_1")
for col in df.columns:
    df = df.rename(columns={col: "Estadisticas - " + col})
df_merge = df_merge.merge(df, how="outer", left_index=True, right_index=True)

df_merge['Estadisticas - Superficie del área de ventas - m2'].value_counts()


df_merge['country'] = 'Argentina'
alphacast.datasets.dataset(131).upload_data_from_df(df_merge, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

