import MySQLdb as mdb
import json
from struct import unpack
import datetime as d
import socket
import asyncio

with open("./config.json") as f:
    json = json.load(f)
    rest_sym = json['restsym']
    usd_rate = json['usdrate']
    usd_check = json['usdcheck']
    indices = json['indices']
begin = "2018-07-09 00:00:00"
end = "2018-07-15 00:00:00"
eod_rate_temp_list = []
eod_rate_collection = {}

'''

function about fetching eod rate from MT4 server web API

1. make time in the request independent from combined symbol's time, allowning jet lag 
2. remove coupling between API request and client trades records, using sql query to config out 
    required symbol pair and date

'''


def get_response(symbol: str, from_t: int, to_t: int, step: int):
    if step not in [1, 15, 60]:
        return b''
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(('mt4demomaster.rztrader.com', 443))
    send_str = ('WHISTORYNEW-symbol={}|period={}|from={}|to={}\nQUIT\n'
                .format(symbol, str(step), str(from_t), str(to_t))).encode('ascii')
    client.send(send_str)
    return client.recv(10148)


async def rate_at_eod(symbol: str, trade_date: str, next_day: int = 0, eod_hour: int = 23):
    if 'USD' == symbol:
        return {
            "symbol": symbol,
            "origin_time": trade_date,
            "eod_time": '',
            "rate": 1.0
        }
    original_date = d.datetime.fromisoformat(trade_date)
    eod_date = int(d.datetime(
        original_date.year, original_date.month, (
            original_date.day + next_day),
        eod_hour, 0, 0).timestamp()
    )

    response = get_response(symbol + '.rp', eod_date, eod_date, 60)
    if len(response) == 4 or len(response) == 0:
        return {
            "symbol": symbol,
            "origin_time": trade_date,
            "eod_time": '',
            "rate": 0.0
        }
    else:
        header = unpack('iii', response[:12])
        # print(header)
        digit = 10 ** (header[1]*(-1))
        body = unpack('iiiii', response[12:][:20])
        return {
            "symbol": symbol,
            "origin_time": trade_date,
            "eod_time": d.datetime.fromtimestamp(body[0]),
            "rate": (body[1] + body[4]) * digit
        }


def create_async_task_list(f_time: str, e_time: str):
    tasks = []
    symbol_date_pair = []
    con = mdb.connect('mt4-report-db02.onezero.com', 'RuizeanRO',
                      'RZRO123!', 'ozhosted-ruizeanmt4live1')
    cur = con.cursor()
    try:
        cur.execute(
            "select substring_index(t.symbol, '.', 1), date(t.close_time) from MT4_TRADES as t where t.close_time between date(%s) and date(%s) and t.symbol != '' and t.conv_rate1 != 0.0 group by t.symbol, date(t.close_time);", [f_time, e_time])
        for i in cur.fetchall():
            s = i[0]
            t = i[1].isoformat()
            if 6 == len(s):
                if "USD" != s[:3] and "USD" != s[3:]:
                    try:
                        alter_s = usd_check[s[:3]].split('.')[0]
                        temp = (alter_s, t)
                    except KeyError:
                        print('error symbol {}'.format(alter_s))
                        continue
                else:
                    temp = (s, t)
            else:
                try:
                    alter_s = indices[s]
                    temp = (alter_s, t)
                except KeyError:
                    print('error symbol {}'.format(alter_s))
                    continue
            if temp not in symbol_date_pair:
                symbol_date_pair.append(temp)
            else:
                continue

        cur.execute(
            "select substring_index(t.symbol, '.', 1), date(t.open_time) from MT4_TRADES as t where t.open_time between date(%s) and date(%s) and t.symbol != '' and t.conv_rate1 != 0.0 group by t.symbol, date(t.open_time);", [f_time, e_time])
        for i in cur.fetchall():
            s = i[0]
            t = i[1].isoformat()
            if 6 == len(s):
                if "USD" != s[:3] and "USD" != s[3:]:
                    try:
                        alter_s = usd_check[s[:3]].split('.')[0]
                        temp = (alter_s, t)
                    except KeyError:
                        print('error symbol {}'.format(alter_s))
                        continue
                else:
                    temp = (alter_s, t)
            else:
                try:
                    alter_s = indices[s]
                    temp = (alter_s, t)
                except KeyError:
                    print('error symbol {}'.format(alter_s))
                    continue
            if temp not in symbol_date_pair:
                symbol_date_pair.append(temp)
            else:
                continue
    except:
        print('database error in get_valid_user function')
        con.rollback()
    finally:
        con.close()
    # return symbol_date_pair
    if 0 != len(symbol_date_pair):
        for pair in symbol_date_pair:
            tasks.append(asyncio.ensure_future(
                rate_at_eod(pair[0], pair[1], 0, 23)))
        return tasks
    else:
        return []


def set_eod_rate_collection(f_time: str, e_time: str):
    now = d.datetime.now().timestamp()
    results = {}
    asnyc_eod_tasks_list = []
    asnyc_eod_tasks_list = create_async_task_list(f_time, e_time)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(asnyc_eod_tasks_list))
    for task in asnyc_eod_tasks_list:
        results["{}&{}".format(task.result()['symbol'], task.result()[
                               'origin_time'])] = task.result()['rate']
    print(d.datetime.now().timestamp() - now)
    return results


def main():

    # print(eod_trades_list_gener())
    # vol = get_trade_list(get_valid_user(['2001']))
    # print(len(vol))
    print(set_eod_rate_collection('2018-07-01', '2018-07-30'))


if __name__ == '__main__':
    main()
