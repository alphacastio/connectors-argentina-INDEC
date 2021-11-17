#!/usr/bin/env python
# coding: utf-8

import pandas as pd

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_industria_maquinaria_agricola.xls"
df1 = pd.read_excel(url, sheet_name="Cuadro 1" )
df2 = pd.read_excel(url, sheet_name="Cuadro 2" )
df3 = pd.read_excel(url, sheet_name="Cuadro 3.1" )
df4 = pd.read_excel(url, sheet_name="Cuadro 4" )
df5 = pd.read_excel(url, sheet_name="Cuadro 5" )
df7 = pd.read_excel(url, sheet_name="Cuadro 7" )
df8 = pd.read_excel(url, sheet_name="Cuadro 8" )


name1 = "Venta de máquinas agrícolas - Facturación total"
df1 = df1.dropna(subset=["Unnamed: 2"]).iloc[2:][["Unnamed: 2"]]
df1.columns = [name1]
df1["Date"] = pd.date_range(start='1/1/2016', periods=len(df1), freq = "Q")
df1 = df1.set_index(df1["Date"])
del df1["Date"]


df2 = df2.dropna(subset=["Unnamed: 2"]).iloc[1:]
df2 = df2.dropna(axis=1).drop('Cuadro 2. Venta de cosechadoras nacionales e importadas. Años 2016-2021', 1)
df2.columns = ['Cosechadoras - Unidades vendidas - Total' ,
'Cosechadoras - Unidades vendidas - Nacionales' ,
'Cosechadoras - Unidades vendidas - Importadas' ,
'Cosechadoras - Facturación - Total' ,
'Cosechadoras - Facturación - Nacionales' ,
'Cosechadoras - Facturación - Importadas' ,
'Cosechadoras - Precio promedio por unidad - Nacionales' ,
'Cosechadoras - Precio promedio por unidad - Importadas']
df2.index = pd.date_range(start='1/1/2016', periods=len(df2), freq = "Q")
df2.index.names = ["Date"]


df3 = df3.dropna(subset=["Unnamed: 2"]).iloc[1:]
df3 = df3.dropna(axis=1).drop('Cuadro 3.1. Venta de tractores nacionales e importados. Años 2016-2021', 1)
df3.columns = ['Tractores - Unidades vendidas - Total' ,
'Tractores - Unidades vendidas - Nacionales' ,
'Tractores - Unidades vendidas - Importadas' ,
'Tractores - Facturación - Total' ,
'Tractores - Facturación - Nacionales' ,
'Tractores - Facturación - Importadas' ,
'Tractores - Precio promedio por unidad - Nacionales' ,
'Tractores - Precio promedio por unidad - Importadas' ,]
df3.index = pd.date_range(start='1/1/2016', periods=len(df3), freq = "Q")
df3.index.names = ["Date"]


df5 = df5.dropna(subset=["Unnamed: 2"]).iloc[1:]
df5 = df5.dropna(axis=1).drop('Cuadro 5. Venta de implementos nacionales e importados. Años 2016-2021', 1)
df5.columns = ['Implementos - Unidades vendidas - Total' ,
'Implementos - Unidades vendidas - Nacionales' ,
'Implementos - Unidades vendidas - Importadas' ,
'Implementos - Facturación - Total' ,
'Implementos - Facturación - Nacionales' ,
'Implementos - Facturación - Importadas' ,
'Implementos - Precio promedio por unidad - Nacionales' ,
'Implementos - Precio promedio por unidad - Importadas']
df5.index = pd.date_range(start='1/1/2016', periods=len(df5), freq = "Q")
df5.index.names = ["Date"]


df4 = df4.dropna(subset=["Unnamed: 3"]).dropna(axis=1)
names = "Sembradoras - " + df4.T[1] + " - " + df4.T[4]
names = names.T
df4.columns = names
df4 = df4.iloc[2:]
df4.index = pd.date_range(start='1/1/2016', periods=len(df4), freq = "Q")
df4.index.names = ["Date"]


df6 = pd.read_excel(url, sheet_name="Cuadro 6" )
df6 = df6.dropna(subset=["Unnamed: 1"]).ffill(axis=1)
names = "Pulverizadoras - " + df6.T[2] + " - " + df6.T[1] + " - " + df6.T[3]
names = names.T
df6.columns = names
df6 = df6.iloc[3:]
df6.index = pd.date_range(start='1/1/2016', periods=len(df6), freq = "Q")
df6.index.names = ["Date"]
df6 = df6.replace("...", "0")
df6 = df6.iloc[: , 1:]


df7 = df7.dropna(subset=["Unnamed: 3"]).dropna(axis=1)
names = "Implementos de accarreo y almacenaje de granos - " + df7.T[2] + " - " + df7.T[4]
names = names.T
df7.columns = names
df7 = df7.iloc[2:]
df7.index = pd.date_range(start='1/1/2016', periods=len(df7), freq = "Q")
df7.index.names = ["Date"]


df8 = df8.dropna(subset=["Unnamed: 3"]).dropna(axis=1)
names = "Otros implementos - " + df8.T[1] + " - " + df8.T[3]
names = names.T
df8.columns = names
df8 = df8.iloc[2:]
df8.index = pd.date_range(start='1/1/2016', periods=len(df8), freq = "Q")
df8.index.names = ["Date"]



df = df1.merge(df2, right_index=True, left_index=True).merge(df3, right_index=True, left_index=True).merge(df4, right_index=True, left_index=True).merge(df5, right_index=True, left_index=True).merge(df6, right_index=True, left_index=True).merge(df7, right_index=True, left_index=True).merge(df8, right_index=True, left_index=True)
df["country"] = "Argentina"
df = df.loc[:,~df.columns.duplicated()]

alphacast.datasets.dataset(645).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

