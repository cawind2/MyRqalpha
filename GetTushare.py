# coding:utf-8
import tushare as ts
import pandas as pd
import pymssql
import datetime
from sqlalchemy import create_engine

df = ts.get_k_data('000001',index=True)
df1 =ts.get_stock_basics()

# 此脚本每天早上0730运行
# 1.取最新的上证综指
# 2.取最新的上市公司基本信息
# 3.取港资持股的股票前2个月价格信息（持股信息由.net程序更新(早上0700运行），存放在csv文件中，然后由.net程序(早上0800）读入数据库

# engine = create_engine("mssql+pymssql://sa:123456@127.0.0.1/F10_PICK",encoding='gbk')
engine = create_engine("mssql+pymssql://sa:123456@127.0.0.1/F10_PICK")
conn = engine.connect()
conn.execute('delete from tb_sh_index')
conn.execute('delete from tb_stock_basics')
conn.close()

df.to_sql('tb_sh_index',engine,if_exists='append',index=False)
df1.to_sql('tb_stock_basics',engine,if_exists='append',index=True)

s='''update tb_stock_basics
set tb_stock_basics.name=tb_stock_info.name,
tb_stock_basics.industry=tb_stock_info.industry,
tb_stock_basics.area=tb_stock_info.area
from tb_stock_basics,tb_stock_info
where tb_stock_basics.code=tb_stock_info.code
'''
conn = engine.connect()
conn.execute(s)
conn.close()
#conn = engine.connect()


now=datetime.datetime.now()
delta1=datetime.timedelta(days=1)
delta2=datetime.timedelta(days=60)
yesterday=now-delta1
twomonthago=now-delta2
yesterdayst=yesterday.strftime('%Y-%m-%d')
twomonthagost=twomonthago.strftime('%Y-%m-%d')

# 取港资持股股票价格, 2个月的价格,存放在csv文件中，然后由.net程序导入数据库，重复数据不导入
sql='select code from tb_hk_shareholder where [date]=' + '\''+yesterdayst+'\''
conn = engine.connect()
df = conn.execute(sql)
allname=df.fetchall()
conn.close()

for each in allname:
    code=each[0][1:7]
    df2=ts.get_k_data(code,start=twomonthagost,end=yesterdayst,index=False)
    df2.to_csv('e:/tempstock.csv',mode='a',header=None,index=False)
'''
s = s1+s2+s3+s4+s5
# s.format(startdate,enddate)
conn = engine.connect()
#if(issql=='y'):
#    conn.execute('delete from tb_temp_stock')
df = conn.execute(s)
allname = df.fetchall()
if(allname.__len__()>0):
    conn.execute('delete from tb_temp_stock')
conn.close()

# df=ts.get_k_data(a,start='2018-01-02',end='2018-01-15',index=False)
# csvfile = open('d:/tempstock.csv','wb')
# writer=csv.writer(csvfile)
for each in allname:
    print each[0][1:7]
    code=each[0][1:7]
    df=ts.get_k_data(code,start=twomonthagost,end=yesterdayst,index=False)
    df.to_sql('tb_temp_stock',engine,if_exists='append',index=False)
    # df.to_csv('e:/tempstock.csv',mode='a',header=None,index=False)



#df = conn.execute('select * from tb_qf2_trade')
#print(df.fetchall())
#conn.close()

##conn = pymysql.connect('.\SQLEXPRESS','sa','123456', 'F10_PICK')
# conn = pymysql.connect(host='.\SQLEXPRESS',user='sa',password='123456',database='F10_PICK')

conn = pymssql.connect(host='.',database='F10_PICK')

c1 = conn.cursor()
c1.execute('select * from tb_qf2_trade')
print(c1.fetchall())
conn.close()

df = ts.get_k_data('000001',index=True)
df.to_sql()
'''
