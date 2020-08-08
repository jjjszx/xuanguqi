import baostock as bs
import pandas as pd
import datetime
import time
from mysqlExt import MySql
import mynotice
# import matplotlib.pyplot as plt

objMysql = MySql()


def computeKDJ(code, startdate, enddate):

    # 获取股票日K线数据
    rs = bs.query_history_k_data(code,
                                 "date,code,high,close,low,tradeStatus",
                                 start_date=startdate, end_date=enddate,
                                 frequency="d", adjustflag="3")
    # 打印结果集
    result_list = []

    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        result_list.append(rs.get_row_data())
        df_init = pd.DataFrame(result_list, columns=rs.fields)
        # 剔除停盘数据
        df_status = df_init[df_init['tradeStatus'] == '1']
        low = df_status['low'].astype(float)
        del df_status['low']
        df_status.insert(0, 'low', low)
        high = df_status['high'].astype(float)
        del df_status['high']
        df_status.insert(0, 'high', high)
        close = df_status['close'].astype(float)
        del df_status['close']
        df_status.insert(0, 'close', close)
        # 计算KDJ指标,前9个数据为空
        low_list = df_status['low'].rolling(window=9).min()
        high_list = df_status['high'].rolling(window=9).max()
        rsv = (df_status['close'] - low_list) / (high_list - low_list) * 100
    df_data = pd.DataFrame()
    df_data['K'] = rsv.ewm(com=2).mean()
    df_data['D'] = df_data['K'].ewm(com=2).mean()
    df_data['J'] = 3 * df_data['K'] - 2 * df_data['D']
    df_data['date'] = df_status['date'].values
    df_data.index = df_status['date'].values
    df_data.index.name = 'date'

    # 删除空数据
    df_data = df_data.dropna()
    # 计算KDJ指标金叉、死叉情况
    df_data['x'] = ''

    kdj_position = df_data['K'] > df_data['D']

    df_data.loc[kdj_position[(kdj_position == True) & (kdj_position.shift() == False)].index, 'x'] = 'j'
    df_data.loc[kdj_position[(kdj_position == False) &(kdj_position.shift() == True)].index, 'x'] = 's'

    return (df_data)

j_dic = {}
s_dic = {}
if __name__ == '__main__':
    login_result = bs.login(user_id='anonymous', password='123456')
    print(login_result.error_msg)
    sql = "select code,code_name from zz500 limit 10"
    rows = objMysql.getRows(sql)

    startdate = (datetime.datetime.now()-datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    enddate = time.strftime("%Y-%m-%d", time.localtime())

    for i in range(len(rows)):
        code = rows[i][0]
        print(code)
        df = computeKDJ(code, startdate, enddate)

        for index, row in df.iterrows():
            for row in df.itertuples(index=True, name='Pandas'):
                x = getattr(row, 'x')
                x_date = getattr(row, 'date')
                if enddate == x_date:
                    if x == '':
                        continue
                    if x == 'j':
                        j_dic[code] = {}
                        j_dic[code][x_date] = x_date

                    if x == 's':
                        s_dic[code] = {}
                        s_dic[code][index] = index

    print(s_dic)
    slist = '死叉: '
    jlist = '金叉: '
    for (k,v) in s_dic.items():
        sql = "select code_name from zz500 where code='"+k+"'"
        code_name = objMysql.getFirstRowColumn(sql)
        slist += k+': '+code_name+', '
    for (k,v) in j_dic.items():
        sql = "select code_name from zz500 where code='" + k + "'"
        code_name = objMysql.getFirstRowColumn(sql)
        jlist += k+': '+code_name+', '


    print(slist[:-2])
    print(jlist[:-2])

    str = slist[:-2]+"\r\n"+jlist[:-2]
    mynotice.send_notice(str)
    bs.logout()
    exit()