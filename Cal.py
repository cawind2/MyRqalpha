# coding:utf-8
import tushare as ts
import pandas as pd
import pymssql
import csv
from sqlalchemy import create_engine

engine = create_engine("mssql+pymssql://sa:123456@127.0.0.1/F10_PICK")
conn = engine.connect()
df = conn.execute('select [code] from tb_cal_code')

allname = df.fetchall()
conn.close()
csvfile = open('d:/holder1.csv','wb')
writer=csv.writer(csvfile)
for each in allname:
    print each[0]
    df = ts.get_k_data(each[0],start='2017-05-10',end='2017-05-10')
    if df.__len__() > 0:
        close1 = df['close'].iloc[0]
    else:
        close1 = 1
    df = ts.get_k_data(each[0],start='2017-12-28',end='2017-12-28')
    if df.__len__() >0:
        close2=df['close'].iloc[0]
    else:
        close2 = 1

    v=(close2-close1)/close1
    print v
    writer.writerow([each[0],close1,close2,v])
csvfile.close()
pass

