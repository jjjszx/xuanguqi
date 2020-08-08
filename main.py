from get_his_data import Kdata
from mysqlExt import MySql
import time

objMysql = MySql()
objMysql.query('set names utf8mb4')

print("<< Start @ :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) ,">>")

sql = "select code from zz500"
rows = objMysql.getRows(sql)

Kd = Kdata()

for i in range(len(rows)):
    code = rows[i][0]
    date = time.strftime("%Y-%m-%d", time.localtime())
    data = Kd.get_k_data(code, '2020-01-01', date)
    sql = "update zz500 set data = '{}' where code = '{}'". format(data, code)
    objMysql.query(sql)

Kd.logout()

print("<< End @ :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) ,">>")
exit()