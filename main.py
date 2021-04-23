# -*- coding: utf-8 -*-
import functions
import pandas as pd
import datetime
import numpy as np
from constantes import diccionario_orange
from pathlib import Path
import os
import re
from os import listdir

input_path = './input/'
osp_daily_path = './input/' + \
    [file for file in listdir(input_path) if re.search('B2C', file)][0]
jz_path = './input/' + \
    [file for file in listdir(input_path) if re.search('azztel', file)][0]
output_path = './output/'

print('Cargando base OSP ----->', osp_daily_path)
xls = pd.ExcelFile(osp_daily_path)

def main():
    print("\nGenerando documento ventas + migras por marca ......\n")
    total_orange = functions.deepDaily(xls,'total_orange',diccionario_orange).fillna(0)
    total_amena = functions.deepDaily(xls,'total_amena',diccionario_orange).fillna(0)
    total_jazztel = functions.jazztel(jz_path)

    total_orange['osp_mov_only'] = total_orange['osp_mov_only']+total_orange['LTE']
    total_orange.drop(columns=['LTE'],axis=1,inplace=True)

    total_orange['orange']=total_orange.sum(axis=1)
    total_amena['amena']=total_amena.sum(axis=1)

    migras_df = pd.read_csv(
        output_path+'migras/'+'migras.csv', sep=',', decimal=',', encoding='CP1252')
    migras_df.fecha_dia = pd.to_datetime(migras_df.fecha_dia)
    migras_df.set_index('fecha_dia', inplace=True)
    migras_df = migras_df.astype({'migras': 'float64'})

    orange_migras = migras_df[['migras']]

    ventas = pd.concat([total_orange, orange_migras, total_jazztel, total_amena], axis=1, sort=False).fillna(0)
    ventas['orange'] = ventas.orange + ventas.migras
    ventas.index = pd.to_datetime(ventas.index)
    ventas.index.name = 'fecha'

    ventas['semana'] = ventas.index.to_series().dt.isocalendar().week
    ventas['fecha_dia'] = ventas.index
    ventas['dia_semana'] = ventas.fecha_dia.dt.weekday
    ventas['timedelta'] = ventas.dia_semana.apply(
        lambda x: pd.Timedelta(days=x))
    ventas['fecha_semana'] = ventas.fecha_dia - ventas.timedelta
    ventas['month'] = (ventas.fecha_semana + pd.Timedelta(days=3)).dt.month
    ventas['year'] = (ventas.fecha_semana + pd.Timedelta(days=3)).dt.year
    ventas.drop(columns=['fecha_dia', 'timedelta',
                        'fecha_semana', 'dia_semana'], inplace=True)

    ventas[['osp_fbb_convergencia', 'osp_fbb_stand_alone', 'osp_mov_convergencia',
       'osp_mov_only',  'migras','orange', 'jazztel', 'amena_fbb', 'amena_mov',
       'amena', 'semana', 'month', 'year']].to_csv(f'{output_path}ventas/ventas{datetime.date.today().strftime("%d%m%y")}.csv',
                  decimal=",", encoding='CP1252')
    ventas[['osp_fbb_convergencia', 'osp_fbb_stand_alone', 'osp_mov_convergencia',
       'osp_mov_only',  'migras','orange', 'jazztel', 'amena_fbb', 'amena_mov',
       'amena', 'semana', 'month', 'year']].to_csv(f'{output_path}ventas/ventas.csv',
                  decimal=",", encoding='CP1252')
    print(
        f'Archivo guardado [{output_path}ventas_{datetime.date.today().strftime("%d%m%y")}.csv]')
    
    print('\nGenerando archivo ventas detalle OSP\n')
    total_osp = functions.deepDaily(
        xls, 'mix_orange_2', diccionario_orange)

    total_osp.reset_index(inplace=True)
    total_osp['dia_semana'] = total_osp['index'].dt.weekday
    total_osp['year'] = total_osp['index'].dt.year
    total_osp['month'] = total_osp['index'].dt.month
    total_osp['timedelta'] = total_osp.dia_semana.apply(
        lambda x: pd.Timedelta(days=x))
    total_osp['fecha_semana'] = total_osp['index'] - total_osp.timedelta

    total_osp.drop(columns=['dia_semana', 'timedelta'], inplace=True)
    final_0 = total_osp.groupby(
        by=['fecha_semana', 'year', 'month']).sum().reset_index()
    final = pd.melt(final_0, id_vars=[
                    'fecha_semana', 'year', 'month'], var_name="variables", value_name='total')

    final.to_csv(f'{output_path}ventas/total_orange.csv',
                decimal=",", encoding='CP1252', index=False)
    
    print('Done')
    print('\nGenerando archivo ventas detalle Jazztel\n')
    mix_squad = functions.deepDaily(
        jz_path, 'mix_squad', diccionario_orange).fillna(0)
    mix_squad.reset_index(inplace=True)
    mix_squad['dia_semana'] = mix_squad['index'].dt.weekday
    mix_squad['year'] = mix_squad['index'].dt.year
    mix_squad['month'] = mix_squad['index'].dt.month
    mix_squad['timedelta'] = mix_squad.dia_semana.apply(
        lambda x: pd.Timedelta(days=x))
    mix_squad['fecha_semana'] = mix_squad['index'] - mix_squad.timedelta
    mix_squad.drop(columns=['dia_semana', 'timedelta'], inplace=True)
    final_squad_0 = mix_squad.groupby(
        by=['fecha_semana', 'year', 'month']).sum().reset_index()
    final_squad = pd.melt(final_squad_0, id_vars=[
        'fecha_semana', 'year', 'month'], var_name="variables", value_name='total')

    final_squad.to_csv(f'{output_path}ventas/total_jazztel.csv',
                       decimal=",", encoding='CP1252', index=False)
    print('Done')


if __name__ == "__main__":
    main()
