import functions
import pandas as pd
import datetime 
import numpy as np

osp_migras_path = './input/CdM_Pospago.xlsx'
osp_daily_path = './input/daily.xlsx'
jz_path = './input/daily_jazztel.xlsx'
output_path = './output/'


def main():
    print("Generando documento ......\n")
    fbb_sa = functions.daily(osp_daily_path, sheet = 'FBB Stand Alone', row = 0, name = 'osp_fbb_stand_alone' )
    fbb_co = functions.daily(osp_daily_path, sheet = 'FBB Convergencia', row = 0, name = 'osp_fbb_convergencia')
    mov_co = functions.daily(osp_daily_path, sheet = 'Móvil Convergencia', row = 0, name = 'osp_mov_convergencia')
    mov_on = functions.daily(osp_daily_path, sheet = 'Mobile Only', row = 0, name = 'osp_mov_only')
    amena_fbb = functions.daily(osp_daily_path, sheet = 'FBB AMENA', row = 10, name = 'amena_fbb')
    amena_mov = functions.daily(osp_daily_path, sheet = 'Total Móvil', row = 8, name = 'amena_mov')
    osp_mig = functions.migras(osp_migras_path)
    jazztel = functions.jazztel(jz_path)
    orange = pd.concat([fbb_co,fbb_sa,mov_co,mov_on,osp_mig], axis=1, sort=False).fillna(0)
    orange['orange']=orange.iloc[:,0]+orange.iloc[:,1]+orange.iloc[:,2]+orange.iloc[:,3]+orange.iloc[:,4]
    amena = pd.concat([amena_fbb, amena_mov], axis = 1, sort = False).fillna(0)
    amena['amena'] = amena.iloc[:,0]+amena.iloc[:,1]
    ventas = pd.concat([orange,jazztel,amena], axis = 1, sort = False).fillna(0)
    ventas.index = pd.to_datetime(ventas.index)
    ventas.index = ventas.asfreq('d').index
    semanas = ventas.index.to_series().dt.week
    ventas['semana'] = semanas
    ventas.index.name = 'fecha'
    s = pd.read_csv('./semanas.csv',sep=";")
    s['fecha'] = pd.to_datetime(s.fecha)
    s.set_index('fecha', inplace=True)
    idx = pd.date_range(s.index.min(), s.index.max())
    s.index = pd.DatetimeIndex(s.index)
    s = s.reindex(idx, fill_value = np.nan).ffill().astype('int64')
    s.index.name = 'fecha'
    merge=pd.merge(ventas,s, how='left', left_index=True, right_index=True)
    merge.to_csv(f'{output_path}ventas_{datetime.date.today().strftime("%d%m%y")}.csv', decimal=",",encoding='CP1252')
    merge.to_csv(f'{output_path}ventas.csv', decimal=",",encoding='CP1252')
    print(f'Archivo guardado [{output_path}ventas_{datetime.date.today().strftime("%d%m%y")}.csv]')
if __name__ == "__main__":
    main()