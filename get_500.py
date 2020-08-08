import baostock as bs
import pandas as pd
from mysqlExt import MySql

objMysql = MySql()
objMysql.query('set names utf8mb4')

# 登陆系统
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

# 获取中证500成分股
rs = bs.query_zz500_stocks()
print('query_zz500 error_code:'+rs.error_code)
print('query_zz500  error_msg:'+rs.error_msg)

# 打印结果集
zz500_stocks = []
while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
    data = rs.get_row_data()
    sql = "insert into zz500 (code, code_name, updateDate) VALUE ('{}','{}','{}')".format(data[1], data[2], data[0])
    print(sql)
    objMysql.query(sql)

# 登出系统
bs.logout()