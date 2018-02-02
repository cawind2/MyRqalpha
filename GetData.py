# coding:utf-8
import tushare as ts
import pandas as pd
import pymysql
import DataBase as db
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://tradeuser:123456@127.0.0.1/py_trade?charset=utf8')

# 取所有的股票基本面
# df = ts.get_stock_basics()
# df.to_csv('e:/s1.csv')
# df1.to_csv('e:/stockbas.csv', mode='a',header=True,index=True )
# 然后用SQL工具导入到 tb_stock_basics

#  https://mp.weixin.qq.com/s?__biz=MzAwOTgzMDk5Ng==&mid=2650833972&idx=1&sn=4de9f9ee81bc8bf85d1e0a4a8f79b0de&chksm=80adb30fb7da3a19817c72ff6f715ee91d6e342eb0402e860e171993bb0293bc4097e2dc4fe9&mpshare=1&scene=1&srcid=1106BPAdPiPCnj6m2Xyt5p2M#wechat_redirect
# 使用新的ts.get_k_data('000001', index=True)

conn, cur = db.connDB()
cur = db.exeQuery(cur,'select * from stock_basics')

# 修改表的date类型，改为varchar
allname = cur.fetchall()
for each in allname:
    if each[0] != 'stock_basics':
        print each[0]
        try:
            sta = db.exeQuery(cur,"ALTER TABLE `%s` CHANGE `date` `date` VARCHAR(25) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL;" % each[0])
        except:
            pass


# 修改表,加主键
allname = cur.fetchall()
for each in allname:
    if each[0] != 'stock_basics':
        print each[0]
        try:
            sta = db.exeQuery(cur,"ALTER TABLE `%s` ADD PRIMARY KEY(`date`);" % each[0])
        except:
            pass


# 清旧数据
cur = db.exeQuery(cur,'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = "py_trade"')
allname = cur.fetchall()
for each in allname:
    print each[0]
    if each[0] != 'stock_basics':
        print each[0]
        sta = db.exeQuery('delete from py_trade.%s; commit; ' % each[0])
        #sta = cur.execute('commit')

# 导入全部历史数据
allname = cur.fetchall()
for each in allname:
    print each[0], each[1]
    df = ts.get_hist_data(each[0])
    if df is not None:
        # df.to_csv('e:/stock/%s.csv' % each[0])
        try:
            # 第一次导入，数据库会报错，date类型要从text改为varchar..是主键不在能text上定义
            df.to_sql(each[0], engine, if_exists='append')
        except:
            pass

# 导入时间段数据
allname = cur.fetchall()
for each in allname:
    print each[0], each[1]
    df = ts.get_hist_data(each[0], start='2016-04-26', end='2016-04-26')
    if df is not None:
        # df.to_csv('e:/stock/%s.csv' % each[0])
        try:
            # 第一次导入，数据库会报错，date类型要从text改为varchar..是主键不在能text上定义
            df.to_sql(each[0], engine, if_exists='append')
        except:
            pass

db.connClose(conn,cur)

