#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests
from bs4 import BeautifulSoup

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

url = 'https://www.indec.gob.ar/Nivel4/Tema/3/35/45'
page = requests.get(url)
soup = BeautifulSoup(page.content, 'html.parser')

#Se busca descargar el archivo
#Estadísticas integradas de balanza de pagos, posición de inversión internacional y deuda externa

link_xls = []
for link in soup.find_all('a'):
    if link.get_text() is not None and 'Estadísticas integradas de balanza de pagos' in link.get_text():
        link_xls.append('https://www.indec.gob.ar' + link.get('href'))

xls_file = requests.get(link_xls[0])

df = pd.read_excel(xls_file.content, sheet_name = "Cuadro 14", skiprows=3, header=[0,1])

df = df.transpose().reset_index()


## ------------------- ##
## Date Extraction ##
df.rename(columns={'level_0': 'year', 'level_1':'month'}, inplace=True)
df["year"] = df["year"].str.replace("Año " , "", regex=True).str.replace("*" , "", regex=True)
df["month"] = df["month"].str.replace("IV" , "10")
df["month"] = df["month"].str.replace("III" , "07")
df["month"] = df["month"].str.replace("II" , "04")
df["month"] = df["month"].str.replace("I" , "01")
df["day"] = 1
df["Date"] = pd.to_datetime(df[["year", "month", "day"]], errors="coerce")
del df["year"]
del df["month"]
del df["day"]
df = df[:3].append(df[3:][df[3:]["Date"].notnull()]).transpose()
## ------------------- ##

df["country"] = df[1] + " - " + df[2]
del df[1]
del df[2]
df = df.set_index("country")

new_header = df.iloc[-1] #grab the first row for the header
df = df[:-1] #take the data less the header row
df.columns = new_header #set the header row as the df header

activos_reserva = df[df["NaT"]=="Q.N.AR.W1.S121.S1.T.A.FA.R.F._Z.USD.X1._X.N"]
df_net_balance = df[df["NaT"].str.split(".", expand=True)[7].isin(["B", "N"])]
df_credits = df[df["NaT"].str.split(".", expand=True)[7].isin(["C"])]
df_debits = df[df["NaT"].str.split(".", expand=True)[7].isin(["D"])]
df_assets = df[df["NaT"].str.split(".", expand=True)[7].isin(["A"])]
df_liab = df[df["NaT"].str.split(".", expand=True)[7].isin(["L"])]

df = df_net_balance.append(activos_reserva)
df = df[df.index.notnull()]

def last_fix(df):
    del df["NaT"]
    df = df.transpose()
    df["Date"] = df.index
    df = df.set_index("Date")
    df["country"] = "Argentina"
    df = df.loc[:, (df != 0).any(axis=0)]
    return df

df = last_fix(df)
df_credits = last_fix(df_credits)
df_debits = last_fix(df_debits)
df_assets = last_fix(df_assets)
df_liab = last_fix(df_liab)

alphacast.datasets.dataset(286).upload_data_from_df(df, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


#Tabla Resumen
#filtro todos los que son hasta nivel 2

df_columns = pd.DataFrame(df.columns)
df_columns[["code", "concept"]] = df_columns["country"].str.split(" - ", expand=True)
df_columns["level"] = df_columns["code"].str.count("\.")

df_summary = df.loc[:, df.columns.isin(df_columns[df_columns["level"]<2]["country"])]

alphacast.datasets.dataset(288).upload_data_from_df(df_summary, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

df_columns_credit = pd.DataFrame(df_credits.columns)
df_columns_credit[["code", "concept"]] = df_columns_credit["country"].str.split(" - ", expand=True)
df_columns_credit["level"] = df_columns_credit["code"].str.count("\.")

df_columns_credit = df_columns_credit.merge(df_columns, how="left", left_on="code", right_on="code")
df_columns_credit["country_y"] = df_columns_credit["country_y"].fillna(df_columns_credit["country_x"]) + " - Credito"
df_credits.columns = df_columns_credit["country_y"]
del df_credits["country - Credito"]
df_credits["country"] = "Argentina"


df_columns_debit = pd.DataFrame(df_debits.columns)
df_columns_debit[["code", "concept"]] = df_columns_debit["country"].str.split(" - ", expand=True)
df_columns_debit["level"] = df_columns_debit["code"].str.count("\.")

df_columns_debit = df_columns_debit.merge(df_columns, how="left", left_on="code", right_on="code")
df_columns_debit["country_y"] = df_columns_debit["country_y"].fillna(df_columns_debit["country_x"]) + " - debito"
df_debits.columns = df_columns_debit["country_y"]
del df_debits["country - debito"]
df_debits["country"] = "Argentina"


df_columns_assets = pd.DataFrame(df_assets.columns)
df_columns_assets[["code", "concept"]] = df_columns_assets["country"].str.split(" - ", expand=True)
df_columns_assets["level"] = df_columns_assets["code"].str.count("\.")

df_columns_liab = pd.DataFrame(df_liab.columns)
df_columns_liab[["code", "concept"]] = df_columns_liab["country"].str.split(" - ", expand=True)
df_columns_liab["level"] = df_columns_liab["code"].str.count("\.")


#Tabla de Servicios
#filtro todos los que son hasta nivel 3

df_columns = pd.DataFrame(df.columns)
df_columns[["code", "concept"]] = df_columns["country"].str.split(" - ", expand=True)
df_columns["level"] = df_columns["code"].str.count("\.")

df_services = df.loc[:, df.columns.isin(df_columns[df_columns["level"]<4]["country"])]
df_services = df_services.loc[:, df_services.columns.str.contains("1.A.b")]

df_services_credit = df_credits.loc[:, df_credits.columns.isin(df_columns_credit[df_columns_credit["level_y"]<4]["country_y"])]
df_services_credit = df_services_credit.loc[:, df_services_credit.columns.str.contains("1.A.b")]

df_services_debit = df_debits.loc[:, df_debits.columns.isin(df_columns_debit[df_columns_debit["level_y"]<4]["country_y"])]
df_services_debit = df_services_debit.loc[:, df_services_debit.columns.str.contains("1.A.b")]

df_services_all = df_services.merge(df_services_credit, how="left", left_index=True, right_index=True).merge(
                    df_services_debit, how="left", left_index=True, right_index=True
                    ) 


df_services_all["country"]  = "Argentina"
alphacast.datasets.dataset(290).upload_data_from_df(df_services_all, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)



df_columns = pd.DataFrame(df.columns)
df_columns[["code", "concept"]] = df_columns["country"].str.split(" - ", expand=True)
df_columns["level"] = df_columns["code"].str.count("\.")

df_capital_account = df.loc[:, df.columns.isin(df_columns[df_columns["level"]<2]["country"])]
df_capital_account = df_capital_account.loc[:, df_capital_account.columns.str[0:2] == "3."]
for col in df_capital_account.columns:
    df_capital_account = df_capital_account.rename(columns={col: col + " - Total"})

df_capital_assets = df_assets.loc[:, df_assets.columns.isin(df_columns_assets[df_columns_assets["level"]<2]["country"])]
df_capital_assets = df_capital_assets.loc[:, df_capital_assets.columns.str[0:2] == "3."]

df_capital_liab = df_liab.loc[:, df_liab.columns.isin(df_columns_liab[df_columns_liab["level"]<2]["country"])]
df_capital_liab = df_capital_liab.loc[:, df_capital_liab.columns.str[0:2] == "3."]


df_capital_all = df_capital_account.merge(df_capital_assets, how="left", left_index=True, right_index=True).merge(
                    df_capital_liab, how="left", left_index=True, right_index=True)



df_capital_all['country'] = 'Argentina'
alphacast.datasets.dataset(291).upload_data_from_df(df_capital_all, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


df_full_all = df.merge(df_credits, how="left", left_index=True, right_index=True).merge( 
                 df_debits, how="left", left_index=True, right_index=True).merge( 
                 df_assets, how="left", left_index=True, right_index=True).merge(
                 df_liab, how="left", left_index=True, right_index=True)
del df_full_all['country_x'] 
del df_full_all['country_y']
df_full_all["country"] = "Argentina"
alphacast.datasets.dataset(298).upload_data_from_df(df_full_all, 
                 deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)

