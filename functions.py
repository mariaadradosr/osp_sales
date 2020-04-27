import pandas as pd
import numpy as np
import datetime

def migras(path):
    df = pd.read_excel(path, sheet_name= 'RESUMEN MKT', header = 2).reset_index()
    df_mig = df.loc[[df[(df['Unnamed: 2']=='MIGRAS x CANAL')|(df['Unnamed: 2']=='MIGRACIONES x CANAL')].index[0]+4]]
    cols_tot = list(df_mig.columns)
    cols = []
    for col in cols_tot: 
            if type(col) == datetime.datetime:
                cols.append(col)
    resultado = df_mig[cols].iloc[[0]].T
    resultado.rename(columns= {list(resultado.columns)[0]:'migras'}, inplace=True)
    return resultado

# def daily(path,sheet = 'FBB Convergencia'):
#     df = pd.read_excel(path, sheet_name= sheet).reset_index()
#     cols_tot = list(df.columns)
#     cols = []
#     for col in cols_tot: 
#         if type(col) == datetime.datetime and col>=datetime.datetime.strptime('2016-05-01 00:00:00', '%Y-%m-%d %H:%M:%S'):
#             cols.append(col)
#     resultado = df[cols].iloc[[0]].T
#     resultado.rename(columns= {0:sheet}, inplace=True)
#     return resultado

def daily(path,sheet = 'FBB Convergencia', row = 0, name = 'column_name'):
    df = pd.read_excel(path, sheet_name= sheet).reset_index()
    cols_tot = list(df.columns)
    cols = []
    for col in cols_tot: 
        if type(col) == datetime.datetime and col>=datetime.datetime.strptime('2016-05-01 00:00:00', '%Y-%m-%d %H:%M:%S'):
            cols.append(col)
    resultado = df[cols].iloc[[row]].T
    resultado.rename(columns= {row:name}, inplace=True)
    return resultado

def jazztel(path):
    df = pd.read_excel(path, sheet_name= 'Mix_Squad').reset_index().loc[[0,25,61]]
    cols_tot = list(df.columns)
    cols = []
    for col in cols_tot: 
            if type(col) == datetime.datetime:
                cols.append(col)
    resultado = df[cols].T
    # resultado.rename(columns= {0:'fbb_s_cdom',25:'fbb_cdom',61:'mov'}, inplace=True)
    resultado['jazztel']= resultado.iloc[:,0:3].sum(axis=1)
    resultado.drop(columns = [0,25,61],inplace= True)
    return resultado
    