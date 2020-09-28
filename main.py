# -*- coding: utf-8 -*-
import functions
import pandas as pd
import datetime
import numpy as np
import constantes
from pathlib import Path
import os
import re

input_path = './input/'
paths = sorted(Path(input_path).iterdir(), key=os.path.getmtime,reverse=True)
migras_file = [path.name for path in paths if re.search('Pospago',path.name)][0]

osp_migras_path = './input/'+migras_file

print(migras_file)

osp_daily_path = './input/orange.xlsx'
jz_path = './input/jazztel.xlsx'
output_path = './output/'

print('loading OSP DB')
xls = pd.ExcelFile(osp_daily_path)
print('OSP DB loaded')


def main():
    print("Generando documento ......\n")
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
    osp_mig = functions.migras(osp_migras_path)
    jazztel = functions.jazztel(jz_path)
    orange = pd.concat([fbb_co, fbb_sa, mov_co, mov_on,
                        osp_mig], axis=1, sort=False).fillna(0)
    orange['orange'] = orange.iloc[:, 0]+orange.iloc[:, 1] + \
        orange.iloc[:, 2]+orange.iloc[:, 3]+orange.iloc[:, 4]
    amena = pd.concat([amena_fbb, amena_mov], axis=1, sort=False).fillna(0)
    amena['amena'] = amena.iloc[:, 0]+amena.iloc[:, 1]
    ventas = pd.concat([orange, jazztel, amena], axis=1, sort=False).fillna(0)
    # ventas = pd.concat([orange, jazztel], axis=1, sort=False).fillna(0)
    ventas.index = pd.to_datetime(ventas.index)
    ventas.index = ventas.asfreq('d').index
    semanas = ventas.index.to_series().dt.week
    ventas['semana'] = semanas
    ventas.index.name = 'fecha'
    s = pd.read_csv('./semanas.csv', sep=";")
    s['fecha'] = pd.to_datetime(s.fecha)
    s.set_index('fecha', inplace=True)
    idx = pd.date_range(s.index.min(), s.index.max())
    s.index = pd.DatetimeIndex(s.index)
    s = s.reindex(idx, fill_value=np.nan).ffill().astype('int64')
    s.index.name = 'fecha'
    merge = pd.merge(ventas, s, how='left', left_index=True, right_index=True)
    merge.to_csv(f'{output_path}ventas_{datetime.date.today().strftime("%d%m%y")}.csv',
                 decimal=",", encoding='CP1252')
    merge.to_csv(f'{output_path}ventas.csv', decimal=",", encoding='CP1252')
    print(
        f'Archivo guardado [{output_path}ventas_{datetime.date.today().strftime("%d%m%y")}.csv]')

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
    total_osp['timedelta'] = total_osp.dia_semana.apply(
        lambda x: pd.Timedelta(days=x))
    total_osp['fecha_semana'] = total_osp['index'] - total_osp.timedelta

    total_osp.drop(columns=['dia_semana', 'timedelta'], inplace=True)
    final_0 = total_osp.groupby(by=['fecha_semana']).sum().reset_index()
    final = pd.melt(final_0,id_vars=["fecha_semana"],var_name="variables",value_name = 'total')

    # final.to_csv(
    #     f'{output_path}total_orange_{datetime.date.today().strftime("%d%m%y")}.csv', decimal=",", encoding='CP1252', index=False)
    final.to_csv(f'{output_path}total_orange.csv',
                     decimal=",", encoding='CP1252', index=False)
    total_osp.to_csv(f'{output_path}total_orange.csv',
                     decimal=",", encoding='CP1252', index=False)


if __name__ == "__main__":
    main()
