# -*- coding: utf-8 -*-
import functions
import pandas as pd
import datetime
import numpy as np
import constantes
from pathlib import Path
import os
import re
from os import listdir

# guardado de prubea

input_path = './input/'
# paths = sorted(Path(input_path).iterdir(), key=os.path.getmtime, reverse=True)
# migras_file = [path.name for path in paths if re.search(
#     'Pospago', path.name)][0]
# osp_migras_path = './input/'+migras_file
# print('\nArchivo migraciones OSP ----->', migras_file, '\n')

osp_daily_path = './input/' + \
    [file for file in listdir(input_path) if re.search('range', file)][0]
jz_path = './input/' + \
    [file for file in listdir(input_path) if re.search('azztel', file)][0]
output_path = './output/'

print('Cargando base OSP ----->', osp_daily_path)
xls = pd.ExcelFile(osp_daily_path)
# xls = pd.read_excel(osp_daily_path,
#                     sheet_name=['FBB Stand Alone', 'FBB Convergencia',
#                                 'Móvil Convergencia', 'Mobile Only', 'Mix canal Amena','Mix canal Osp'],
#                     engine='pyxlsb')


def main():
    print("\nGenerando documento ventas + migras por marca ......\n")
    fbb_sa = functions.daily(xls, sheet='FBB Stand Alone',
                             row=0, name='osp_fbb_stand_alone')
    fbb_co = functions.daily(
        xls, sheet='FBB Convergencia', row=0, name='osp_fbb_convergencia')
    mov_co = functions.daily(
        xls, sheet='Móvil Convergencia', row=0, name='osp_mov_convergencia')
    mov_on = functions.daily(xls, sheet='Mobile Only',
                             row=0, name='osp_mov_only')
    amena_fbb = functions.daily(
        xls, sheet='Mix canal Amena', row=0, name='amena_fbb')
    amena_mov = functions.daily(
        xls, sheet='Mix canal Amena', row=25, name='amena_mov')
    # osp_mig = functions.migras(osp_migras_path)
    migras_df = pd.read_csv(
        output_path+'migras/'+'migras.csv', sep=',', decimal=',', encoding='CP1252')
    migras_df.fecha_dia = pd.to_datetime(migras_df.fecha_dia)
    migras_df.set_index('fecha_dia', inplace=True)
    migras_df = migras_df.astype({'migras': 'float64'})
    osp_mig = migras_df[['migras']]
    jazztel = functions.jazztel(jz_path)
    orange = pd.concat([fbb_co, fbb_sa, mov_co, mov_on,
                        osp_mig], axis=1, sort=False).fillna(0)
    orange['orange'] = orange.iloc[:, 0]+orange.iloc[:, 1] + \
        orange.iloc[:, 2]+orange.iloc[:, 3]+orange.iloc[:, 4]
    amena = pd.concat([amena_fbb, amena_mov], axis=1, sort=False).fillna(0)
    amena['amena'] = amena.iloc[:, 0]+amena.iloc[:, 1]
    ventas = pd.concat([orange, jazztel, amena], axis=1, sort=False).fillna(0)

    ventas.index = pd.to_datetime(ventas.index)
    # ventas.index = ventas.asfreq('d').index
    ventas.index.name = 'fecha'

    # ventas['semana'] = ventas.index.to_series().dt.week
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

    # s = pd.read_csv('./semanas.csv', sep=";")
    # s['fecha'] = pd.to_datetime(s.fecha)
    # s.set_index('fecha', inplace=True)
    # idx = pd.date_range(s.index.min(), s.index.max())
    # s.index = pd.DatetimeIndex(s.index)
    # s = s.reindex(idx, fill_value=np.nan).ffill().astype('int64')
    # s.index.name = 'fecha'
    # merge = pd.merge(ventas, s, how='left', left_index=True, right_index=True)

    ventas.to_csv(f'{output_path}ventas/ventas_{datetime.date.today().strftime("%d%m%y")}.csv',
                  decimal=",", encoding='CP1252')
    ventas.to_csv(f'{output_path}ventas/ventas.csv',
                  decimal=",", encoding='CP1252')
    print(
        f'Archivo guardado [{output_path}ventas_{datetime.date.today().strftime("%d%m%y")}.csv]')

    print('\nGenerando archivo ventas detalle OSP\n')
    fbb_conv = functions.deepDaily(
        xls, 'fbb_conv', constantes.diccionario_orange)
    fbb_only = functions.deepDaily(
        xls, 'fbb_only', constantes.diccionario_orange)
    mov_conv = functions.deepDaily(
        xls, 'mov_conv', constantes.diccionario_orange)
    mov_only = functions.deepDaily(
        xls, 'mov_only', constantes.diccionario_orange)
    mix_canal = functions.deepDaily(
        xls, 'mix_canal', constantes.diccionario_orange)
    total_osp = pd.concat([fbb_conv, fbb_only, mov_conv,
                           mov_only, mix_canal], axis=1, sort=False).fillna(0)

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

    # final.to_csv(
    #     f'{output_path}total_orange_{datetime.date.today().strftime("%d%m%y")}.csv', decimal=",", encoding='CP1252', index=False)
    final.to_csv(f'{output_path}ventas/total_orange.csv',
                 decimal=",", encoding='CP1252', index=False)
    # total_osp.to_csv(f'{output_path}total_orange.csv',
    #                  decimal=",", encoding='CP1252', index=False)
    print('Done')
    print('\nGenerando archivo ventas detalle Jazztel\n')
    mix_squad = functions.deepDaily(
        jz_path, 'mix_squad', constantes.diccionario_orange).fillna(0)
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
