# coding:utf-8
import tushare as ts
import pandas as pd
import pymssql
import datetime
from sqlalchemy import create_engine

df = ts.get_k_data('000001',index=True)
df1 =ts.get_stock_basics()

# 取2017。 4 财务报表
df2=ts.get_report_data(2017,4)
# 取2017。3财务报表， 有些公司会拖到下一年度，所以要更新
# 其它报表不更新了，主要要报表日期 report_date
df2_1 = ts.get_report_data(2017,3)
df2_2 = ts.get_report_data(2017,2)

df3=ts.get_profit_data(2017,4)
df4=ts.get_operation_data(2017,4)
df5=ts.get_growth_data(2017,4)
df6=ts.get_debtpaying_data(2017,4)
df7=ts.get_cashflow_data(2017,4)

# 此脚本每天早上0730运行
# 1.取最新的上证综指
# 2.取最新的上市公司基本信息
# 3.取消了---取港资持股的股票前2个月价格信息（持股信息由.net程序更新(早上0700运行），存放在csv文件中，然后由.net程序(早上0800）读入数据库
# 3。取昨天全天股票行情数据

# engine = create_engine("mssql+pymssql://sa:123456@127.0.0.1/F10_PICK",encoding='gbk')
engine = create_engine("mssql+pymssql://sa:123456@127.0.0.1/F10_PICK")
conn = engine.connect()


conn.execute('delete from tb_sh_index')
conn.execute('delete from tb_stock_basics')

conn.execute('delete from tb_report_data where [date]=' + '\''+'201704'+'\'')
conn.execute('delete from tb_report_data where [date]=' + '\''+'201703'+'\'')
conn.execute('delete from tb_report_data where [date]=' + '\''+'201702'+'\'')

conn.execute('delete from tb_profit_report1')
conn.execute('delete from tb_profit_data1')
conn.execute('delete from tb_operation_data1')
conn.execute('delete from tb_growth_data1')
conn.execute('delete from tb_debtpaying_data1')
conn.execute('delete from tb_cashflow_data1')
conn.close()

df.to_sql('tb_sh_index',engine,if_exists='append',index=False)
df1.to_sql('tb_stock_basics',engine,if_exists='append',index=True)

# df2.to_sql('tb_profit_report1',engine,if_exists='append',index=False)
df2['date']='201704'
df2.to_sql('tb_report_data',engine,if_exists='append',index=False)
df2_1['date'] = '201703'
df2_1.to_sql('tb_report_data',engine,if_exists='append',index=False)
df2_2['date'] = '201702'
df2_2.to_sql('tb_report_data',engine,if_exists='append',index=False)

df3.to_sql('tb_profit_data1',engine,if_exists='append',index=False)
df4.to_sql('tb_operation_data1',engine,if_exists='append',index=False)
df5.to_sql('tb_growth_data1',engine,if_exists='append',index=False)
df6.to_sql('tb_debtpaying_data1',engine,if_exists='append',index=False)
df7.to_sql('tb_cashflow_data1',engine,if_exists='append',index=False)

s='''update tb_stock_basics
set tb_stock_basics.name=tb_stock_info.name,
tb_stock_basics.industry=tb_stock_info.industry,
tb_stock_basics.area=tb_stock_info.area
from tb_stock_basics,tb_stock_info
where tb_stock_basics.code=tb_stock_info.code
'''
conn = engine.connect()
conn.execute(s)

#更新report_date 数据
conn.execute('update tb_report_data set report_date1='+'\''+'2018-'+'\'' + ' + report_date where [date]= ' + '\''+'201704'+'\'')

conn.execute('update tb_report_data set report_date1='+'\''+'2017-'+'\'' + ' + report_date where [date]= ' + '\''+'201703'+'\'' + ' and report_date >= ' + '\''+'10'+'\'' )
conn.execute('update tb_report_data set report_date1='+'\''+'2018-'+'\'' + ' + report_date where [date]= ' + '\''+'201703'+'\'' + ' and report_date < ' + '\''+'10'+'\'' )

conn.execute('update tb_report_data set report_date1='+'\''+'2017-'+'\'' + ' + report_date where [date]= ' + '\''+'201702'+'\'' + ' and report_date >= ' + '\''+'07'+'\'' )
conn.execute('update tb_report_data set report_date1='+'\''+'2018-'+'\'' + ' + report_date where [date]= ' + '\''+'201702'+'\'' + ' and report_date < ' + '\''+'07'+'\'' )

conn.close()
#conn = engine.connect()


now=datetime.datetime.now()
delta1=datetime.timedelta(days=1)
delta2=datetime.timedelta(days=60)
yesterday=now-delta1
twomonthago=now-delta2
yesterdayst=yesterday.strftime('%Y-%m-%d')
twomonthagost=twomonthago.strftime('%Y-%m-%d')

# 取昨天股票数据全部
cal = ts.trade_cal()
isopen = cal.loc[cal['calendarDate']==yesterdayst]
if isopen.loc[isopen.index[0],'isOpen'] == 1:
    dftrade = ts.get_day_all(yesterdayst)
    dftrade['date'] = yesterdayst
    dftrade.to_sql('tb_stock',engine,if_exists='append',index=False)
    pass
#    pass

s1='''update tb_stock
set tb_stock.name=tb_stock_info.name,
tb_stock.industry=tb_stock_info.industry,
tb_stock.area=tb_stock_info.area
from tb_stock,tb_stock_info
where tb_stock.code=tb_stock_info.code
and tb_stock.date=
'''
s2 = '\''+yesterdayst+'\''

conn = engine.connect()
conn.execute(s1+s2)
conn.close()
'''
# 取交易日
cal = ts.trade_cal()
# start from 2017-07-03
istartday = 9693
# end 2018-02-25
iendday= 9930
for each in range(9693,9930):
    if cal.loc[each]['isOpen'] == 1:
        tradedate = cal.loc[each]['calendarDate']
        dftrade = ts.get_day_all(tradedate)
        dftrade['date'] = tradedate
        dftrade.to_sql('tb_stock',engine,if_exists='append',index=False)
        pass
    pass


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
    # df2.to_csv('d:/r1.csv',mode='a',header=None,index=False, encoding='utf8')



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
