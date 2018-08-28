import multiprocessing as mp
import string
import MySQLdb as mdb
from datetime import datetime
import json
from struct import pack, unpack
import datetime as d
import socket
import asyncio
import functools
import numpy as np
import pandas as pd

with open('./setting.json') as ff:
    j = json.load(ff)
    db_usr = j['dbUsr']
    db_pwd = j['dbPwd']
    db_hst = j['dbHst']
    db_name = j['dbNm']
with open('./config.json') as cc:
    j = json.load(cc)
    cz = j['CZ']
    salesList = j['sales']
    usdCheck = j['usdcheck']
    indices = j['indices']

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
        return{
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
        return{
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
        return{
            "symbol": symbol,
            "origin_time": trade_date,
            "eod_time": d.datetime.fromtimestamp(body[0]),
            "rate": (body[1] + body[4]) * digit
        }


def create_symbol_list(f_time: str, e_time: str):
    symbol_date_pair = []
    con = mdb.connect(db_hst, db_usr, db_pwd, db_name)

    cur = con.cursor()
    try:
        cur.execute(
            "select substring_index(t.symbol, '.', 1), date(t.close_time) from MT4_TRADES as t where t.close_time between date(%s) and date(%s) and t.symbol != '' and t.conv_rate1 != 0.0 group by t.symbol, date(t.close_time);", [f_time, e_time])
        for i in cur.fetchall():
            s = i[0]
            t = i[1].isoformat()
            alter_s = ''
            if 6 == len(s):
                if "USD" == s[:3]: continue
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
                    alter_s = s
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
            alter_s = ''
            if 6 == len(s):
                if "USD" == s[:3]: continue
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
                    alter_s = s
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
    return symbol_date_pair


def set_eod_rate_collection(f_time: str, e_time: str):
    now = d.datetime.now().timestamp()
    results = {}
    eod_symbol_list = create_symbol_list(f_time, e_time)
    loop = asyncio.get_event_loop()
    asnyc_eod_tasks_list = [asyncio.ensure_future(rate_at_eod(
        pair[0], pair[1], 0, 23)) for pair in eod_symbol_list]
    loop.run_until_complete(asyncio.wait(asnyc_eod_tasks_list))
    for task in asnyc_eod_tasks_list:
        results["{}&{}".format(task.result()['symbol'], task.result()[
            'origin_time'])] = task.result()['rate']
    print(d.datetime.now().timestamp() - now)
    return results

def rate_ready_to_cal(fromt_t:str,to_t:str):
    fetch_result = set_eod_rate_collection(fromt_t,to_t)
    if {} == fetch_result:
        return
    temp_list = []
    for k in fetch_result.keys():
        pair = k.split('&')[0]
        tr_t = k.split('&')[1]
        init_rate = fetch_result[k]
        final_rate = 0.0
        if "USD" == pair[:3]:
            final_rate = 1/init_rate
        else:
            final_rate = init_rate
        temp_list.append([pair+'.rp', tr_t, init_rate, final_rate])
    final = pd.DataFrame(np.array(temp_list),columns=('pair','time','fetchRate','finalRate'))
    final['fetchRate'] = pd.to_numeric(final['fetchRate'])
    final['finalRate'] = pd.to_numeric(final['finalRate'])
    return final

def main():
    # create_symbol_list('2018-07-09', '2018-07-15')
    with pd.option_context('display.max_rows', None, 'display.max_columns', 6):
        print( rate_ready_to_cal('2018-07-09', '2018-07-15'))
    # output = mp.Queue()
    # processes = [mp.Process(target=qu, args=(output, x)) for x in range(200)]
    # for p in processes:
    #     p.start()
    # for p in processes:
    #     p.join()
    # results = [output.get() for p in processes]
    # print(results)


if __name__ == '__main__':
    main()
