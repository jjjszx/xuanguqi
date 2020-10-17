import baostock as bs
import pandas as pd
import datetime

startdate   = (datetime.datetime.now()-datetime.timedelta(days=50)).strftime("%Y-%m-%d")
enddate     = datetime.datetime.now().strftime("%Y-%m-%d")

# baostock login
lg = bs.login()
# print login error msg
if lg.error_code != '0':
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)

zz500_data = bs.query_zz500_stocks()

# 打印结果集
zz500_stocks = []
while (zz500_data.error_code == '0') & zz500_data.next():
    # 获取一条记录，将记录合并在一起
    row_data = zz500_data.get_row_data()
    code = row_data[1]

    # query history k data
    rs = bs.query_history_k_data_plus(code,
                                     "date,code,close,tradeStatus,open,volume",
                                     start_date=startdate, end_date=enddate,
                                     frequency="d", adjustflag="3")
    # handle k data
    result_list = []
    while (rs.error_code == '0') & rs.next():
        rs_data = rs.get_row_data()

        if rs_data[3] == '1':
            result_list.append(rs_data)

    pd_result = pd.DataFrame(result_list, columns=rs.fields)

    # MA13
    ma=13
    pd_result['ma_'+str(ma)] = pd_result['close'].rolling(ma).mean()

    #
    ma_position = (pd_result['close'].astype('float') > pd_result['ma_13'].astype('float')) & (pd_result['open'].astype('float') > pd_result['ma_13'].astype('float'))

    flag = False
    tmp_unique = ma_position[5:-6].unique()
    if (False in tmp_unique) & (True not in tmp_unique):
        # 突破13日线加关注
        if True in ma_position[-6:].unique():
            if True in ma_position[-3:].unique():
                if (ma_position[-6:].value_counts()[True] > 3) & (ma_position[-3:].value_counts()[True] == 3):
                    # 3-5个交易日不跌破13日均线
                    flag = True

    if flag == False:
        continue

    # TODO volume
