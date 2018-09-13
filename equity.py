import MySQLdb as mdb
from json import load
from numpy import array, unique, dtype, around, float32
from pandas import DataFrame, to_datetime, to_numeric, read_sql_query
from datetime import datetime as d
import inspect

with open('./setting.json') as ff:
    j = load(ff)
    db_usr = j['dbUsr']
    db_pwd = j['dbPwd']
    db_hst = j['dbHst']
    db_name = j['dbNm']

class Equity:
    def __init__(self,login:str,closed_pl:float,total_deposit:float,first_eq:float,last_eq:float):
        self.login = login
        self.closed_pl = closed_pl
        self.total_deposit = total_deposit
        self.equity_movement = around(float32(last_eq - first_eq),4)
        self.float_movement = around(float32(last_eq - first_eq - closed_pl - total_deposit),3)
    def get(self):
        return [self.login,self.closed_pl,self.total_deposit,self.equity_movement,self.float_movement]

def get_equity_data(from_t: str, close_t: str):
    frame = inspect.getframeinfo(inspect.currentframe())
    file_name = frame.filename
    function_name = frame.function
    try:
        temp_begin = d.fromisoformat(from_t)
        d.fromisoformat(close_t)
    except:
        print('Error: iso time format error in function: {}; file: {}'.format(
            function_name, file_name))
        return

    temp_begin_weekday = temp_begin.isoweekday()
    if 1 == temp_begin_weekday:
        pre_begin_diff = 3
    elif 7 == temp_begin_weekday:
        pre_begin_diff = 2
    else:
        pre_begin_diff = 1
    pre_begin = d(temp_begin.year, temp_begin.month,
                  (temp_begin.day - pre_begin_diff))
    pre_begin_string = pre_begin.isoformat().split('T')[0]
    con = mdb.connect(db_hst, db_usr, db_pwd, db_name)
    try:
        equity_records = read_sql_query(
            """select dd.login,date(time) as time,balance,profit_closed,equity,deposit from MT4_DAILY dd join 
            (select login from MT4_TRADES where (open_time between date(%(from)s) and date(%(to)s) or 
            close_time between date(%(from)s) and date(%(to)s)) and conv_rate1 != 0.0 and symbol != '' 
            group by login) as tt on tt.login = dd.login where time between date(%(pre_from)s) and date(%(to)s) 
            order by dd.login, dd.time;""",
            con, params={'pre_from': pre_begin_string, 'from': from_t, 'to': close_t},parse_dates=['time'])
    except:
        print('Error: database query error in function: {}; file: {}'.format(
            function_name, file_name))
        con.rollback()
        return
    equity_records['balance'] = to_numeric(equity_records['balance'])
    equity_records['profit_closed'] = to_numeric(
        equity_records['profit_closed'])
    equity_records['equity'] = to_numeric(equity_records['equity'])
    equity_records['deposit'] = to_numeric(equity_records['deposit'])
    return equity_records

def equity_by_user(raw_data):
    grouped_index = raw_data.groupby(['login']).groups
    temp_cal_list = []
    for key in grouped_index.keys():
        tem_ll = []
        for i in grouped_index[key]:
            tem_ll.append(raw_data.iloc[i])
        temp_dataframe = DataFrame(
                            array(tem_ll),
                            columns=['login','time','balance',
                            'profit_closed','equity','deposit']
                        )
        temp_cal_list.append(Equity(
            login = str(key),
            closed_pl = float32(temp_dataframe['profit_closed'][1:].sum()),
            total_deposit = float32(temp_dataframe['deposit'][1:].sum()),
            first_eq = float32(temp_dataframe['equity'].iloc[0]),
            last_eq = float32(temp_dataframe['equity'].iloc[-1])
        ).get())
                
    return DataFrame(array(temp_cal_list),
                    columns=['login','closed_PL','total_deposit',
                            'equity_movement','float_movement']
                    )

def main():
    # print(get_active_user('2018-03-01', '2018-08-30'))
    raw_data = get_equity_data('2018-03-02', '2018-09-13')
    grouped = equity_by_user(raw_data)
    print(grouped)


if __name__ == '__main__':
    main()
