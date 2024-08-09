from dbfread import DBF
import pyodbc as py
import time
import pandas as pd
import yaml
import sqlalchemy as sa
import urllib


start = time.perf_counter()
with open('.yaml') as f:
     data_yaml = yaml.safe_load(f)

DRIVER_NAME = data_yaml['db_con']['driver']
SERVER_NAME = data_yaml['db_con']['server']
DATABASE_NAME = data_yaml['db_con']['database']
Uid = data_yaml['db_con']['uid']
Pwd = data_yaml['db_con']['pwd']


con_str = '''Driver={};Server={};Database={};Uid ={};
               Pwd ={};Trusted_Connection=yes;Encrypt=no'''.format(DRIVER_NAME, SERVER_NAME,DATABASE_NAME,Uid,Pwd)

#Подключение к базе данных 'fast_executemany=True' - дает значительный прирост в скорости 
quoted = urllib.parse.quote_plus(con_str)
engine = sa.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted), fast_executemany=True)

# Ваш файл .dbf добавляем в вашу базу данных
data = [dict(i) for i in DBF('your_name.dbf')]
df = pd.DataFrame(data) 
df.to_sql('your_name', engine, chunksize=100000, if_exists="replace", index=False)


            
dt_end = time.perf_counter()
print(round(dt_end-start, 2)/60)