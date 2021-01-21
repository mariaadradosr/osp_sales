import pandas as pd
import numpy as np
import datetime


def migras(path):
    df = pd.read_excel(path, sheet_name='RESUMEN MKT', header=2).reset_index()
    df_mig = df.loc[[df[(df['Unnamed: 2'] == 'MIGRAS x CANAL') | (
        df['Unnamed: 2'] == 'MIGRACIONES x CANAL')].index[0]+4]]
    cols_tot = list(df_mig.columns)
    cols = []
    for col in cols_tot:
        if type(col) == datetime.datetime:
            cols.append(col)
    resultado = df_mig[cols].iloc[[0]].T
    resultado.rename(columns={list(resultado.columns)
                              [0]: 'migras'}, inplace=True)
    return resultado


def migras_new(path,file):
    df = pd.read_excel(path, sheet_name='RESUMEN MKT', header=2).reset_index()
    df_mig = df.loc[[df[(df['Unnamed: 2'] == 'MIGRAS x CANAL') | (
        df['Unnamed: 2'] == 'MIGRACIONES x CANAL')].index[0]+4]]
    cols_tot = list(df_mig.columns)
    cols = []
    for col in cols_tot:
        if type(col) == datetime.datetime:
            cols.append(col)
    resultado = df_mig[cols].iloc[[0]].T.reset_index()
    resultado.rename(columns={list(resultado.columns)[0]: 'fecha_dia',
                              list(resultado.columns)[1]: 'migras'
                             }, inplace=True)
    resultado['file'] = file
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


def daily(xls, sheet='FBB Convergencia', row=0, name='column_name'):
    df = pd.read_excel(xls, sheet_name=sheet).reset_index()
    cols_tot = list(df.columns)
    cols = []
    for col in cols_tot:
        if type(col) == datetime.datetime and col >= datetime.datetime.strptime('2016-05-01 00:00:00', '%Y-%m-%d %H:%M:%S'):
            cols.append(col)
    resultado = df[cols].iloc[[row]].T
    resultado.rename(columns={row: name}, inplace=True)
    return resultado

# def daily(xls, sheet='FBB Convergencia', row=0, name='column_name'):
#     df = xls[sheet]
#     cols_tot = list(df.columns)
#     cols = []
#     for col in cols_tot:
#         if type(col) == datetime.datetime and col >= datetime.datetime.strptime('2016-05-01 00:00:00', '%Y-%m-%d %H:%M:%S'):
#             cols.append(col)
#     resultado = df[cols].iloc[[row]].T
#     resultado.rename(columns={row: name}, inplace=True)
#     return resultado

def jazztel(path):
    # Total FBB sin cambio de domicilio + FBB cambio domicilio + total ventas móvil
    df = pd.read_excel( 
        path, sheet_name='Mix_Squad').reset_index().loc[[0, 30, 66]]
    cols_tot = list(df.columns)
    cols = []
    for col in cols_tot:
        if type(col) == datetime.datetime:
            cols.append(col)
    resultado = df[cols].T
    # resultado.rename(columns= {0:'fbb_s_cdom',25:'fbb_cdom',61:'mov'}, inplace=True)
    resultado['jazztel'] = resultado.iloc[:, 0:3].sum(axis=1)
    resultado.drop(columns=[0, 30, 66], inplace=True)
    return resultado


def deepDaily(xls, name, dic):
    df = pd.read_excel(xls, sheet_name=dic[name][0])
    # df = xls[dic[name][0]]
    cols = []
    cols_tot = list(df.columns)
    for col in cols_tot:
        if type(col) == datetime.datetime and col >= datetime.datetime.strptime('2016-05-01 00:00:00', '%Y-%m-%d %H:%M:%S'):
            cols.append(col)
    resultado = df[cols].iloc[list(dic[name][1].keys())].T
    resultado.rename(columns=dic[name][1], inplace=True)
    return resultado



def comparaCampoMigras(row):
    if (row['migras_x'] == row['migras_y']):
        return row['migras_x']
    elif (row['migras_x'] != row['migras_y']) & (row['migras_y'] != 0):
        return row['migras_y']
    elif  (row['migras_x'] != row['migras_y']) & (row['migras_y'] == 0):
        return row['migras_x']
    elif  (row['migras_x'] != row['migras_y']) & (row['migras_x'] == 0):
        return row['migras_y']
def comparaCampoMigrasFile(row):
    if (row['migras_x'] == row['migras_y']) & (row['file_x'] != 0):
        return row['file_x']
    elif (row['migras_x'] == row['migras_y']) & (row['file_x'] == 0):
        return row['file_y']
    elif (row['migras_x'] != row['migras_y']) & (row['migras_y'] != 0):
        return row['file_y']
    elif  (row['migras_x'] != row['migras_y']) & (row['migras_y'] == 0):
        return row['file_x']
    elif  (row['migras_x'] != row['migras_y']) & (row['migras_x'] == 0):
        return row['file_y']