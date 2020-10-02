import pandas as pd
import os
from pathlib import Path
import re
import functions
import constantes


output_path = './output/migras/'
input_path = './input/migras/'
#Comprueba si la base está creada o no. En el caso que esté creada genera un dataframe con el csv y en el contrario la crea.
if 'migras.csv' not in os.listdir(output_path):
    print('Creating migras base........')
    migras_df = pd.DataFrame(data=constantes.migras_base)
    migras_df.fecha_dia = pd.to_datetime(migras_df.fecha_dia)
else:
    migras_df = pd.read_csv(output_path+'migras.csv',sep=',',decimal=',',encoding='CP1252')
    migras_df.fecha_dia = pd.to_datetime(migras_df.fecha_dia)

saved_files =  [e for e in os.listdir(input_path)]
base_files = list(migras_df.file.unique())
to_add = [f for f in saved_files if f not in base_files]

print(to_add)

for f in to_add:
    migras_df_to_add = functions.migras_new(input_path+file+'.xlsx',file = f)
