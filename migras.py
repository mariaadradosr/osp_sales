import pandas as pd
import os
from pathlib import Path
import re
import functions
import constantes


output_path = './output/migras/'
input_path = './input/migras/'

def migras():
    
    saved_files =  [e for e in os.listdir(input_path)]
    
    #Comprueba si la base está creada o no. En el caso que esté creada genera un dataframe con el csv y en el contrario la crea.
    if 'migras.csv' not in os.listdir(output_path):
        print('Creating migras base ........')
        # migras_df = pd.DataFrame(data=constantes.migras_base)
        file = [e for e in os.listdir(input_path)][0]
        print(file)
        migras_df =functions.migras_new('./input/migras/'+file,file)
        migras_df.fecha_dia = pd.to_datetime(migras_df.fecha_dia)
    else:
        print('Loading base ........')
        migras_df = pd.read_csv(output_path+'migras.csv',sep=',',decimal=',',encoding='CP1252')
        migras_df.fecha_dia = pd.to_datetime(migras_df.fecha_dia)

    base_files = list(migras_df.file.unique())
    to_add = [f for f in saved_files if f not in base_files]

    print('\nFiles to add:\n',to_add)

    for f in to_add:
        print('\nAdding ---->',f)
        migras_df_to_add = functions.migras_new(input_path+f,file = f)
        merged_df = pd.merge(migras_df, migras_df_to_add, on = 'fecha_dia',how='outer').fillna(0)
        merged_df['migras'] = merged_df.apply(lambda row: functions.comparaCampoMigras(row), axis = 1)
        merged_df['file'] = merged_df.apply(lambda row: functions.comparaCampoMigrasFile(row), axis = 1)
        migras_df = merged_df[['fecha_dia','migras','file']]

    migras_df.to_csv('./output/migras/migras.csv',index=False)
    print('\nmigras.csv saved')
if __name__ == "__main__":
    migras()

