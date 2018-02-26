# coding:utf-8
import tushare as ts
import pandas as pd
import pymssql
import csv
from sqlalchemy import create_engine

# df = ts.get_day_all('2018-02-22')
# print df.loc[df['code'] == '600903']

engine = create_engine("mssql+pymssql://sa:123456@127.0.0.1/F10_PICK")
# conn = engine.connect()
# conn.execute('delete from tb_temp_stock')
# conn.close()

startdate=raw_input('input startdate:')
enddate=raw_input('input enddate:')
issql=raw_input('export to db(y or n):')

s1 = '''select t1.code,t1.name,t1.shareholder,t1.sharerate,t2.shareholder, CAST(t1.shareholder as float)-cast(t2.shareholder as float) as r, (CAST(t1.shareholder as float)-cast(t2.shareholder as float))/ cast(t2.shareholder as float) as r1 from
(
	select code,name,shareholder,sharerate from tb_hk_shareholder
	where [date]='''
s2 = '\''+startdate+'\''
s3 =''') t1
left join
(
	select code,name,shareholder from tb_hk_shareholder
	where [date]='''
s4 = '\''+enddate+'\''
s5 ='''
) t2
on t1.code=t2.code
order by r1 desc'''
s = s1+s2+s3+s4+s5
s.format(startdate,enddate)
conn = engine.connect()
#if(issql=='y'):
#    conn.execute('delete from tb_temp_stock')
df = conn.execute(s)
allname = df.fetchall()
conn.close()

# df=ts.get_k_data(a,start='2018-01-02',end='2018-01-15',index=False)
# csvfile = open('d:/tempstock.csv','wb')
# writer=csv.writer(csvfile)
for each in allname:
    print each[0][1:7]
    code=each[0][1:7]
    df=ts.get_k_data(code,start=enddate,end=startdate,index=False)
    # df.to_csv('e:/tempstock.csv',mode='a',header=None,index=False)
    if(issql=='y'):
        # df.to_csv('e:/tempstock.csv',mode='a',header=None,index=False)
        df.to_sql('tb_temp_stock',engine,if_exists='append',index=False)
    else:
        df.to_csv('e:/tempstock.csv',mode='a',header=None,index=False)
        # df.to_sql('tb_temp_stock',engine,if_exists='append',index=False)


print 'done!'

