import pymysql


def connDB():
    conn = pymysql.connect(host='127.0.0.1', user='tradeuser', passwd='123456', db='py_trade', charset='utf8')
    cur = conn.cursor()
    return conn, cur


def exeUpdate(cur, sql):
    sta = cur.execute(sql)
    return sta


def exeDelete(cur, IDs):
    for eachID in IDs.split(' '):
        sta = cur.execute('delete from %s where ')
    return sta


def exeQuery(cur,sql):
    cur.execute(sql)
    return  cur


def connClose(conn, cur):
    cur.close()
    conn.close()



