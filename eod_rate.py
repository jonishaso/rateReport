import MySQLdb as mdb
from datetime import datetime as d
from json import load
from struct import unpack
from socket import socket, AF_INET, SOCK_STREAM

with open('./setting.json') as ff:
    j = load(ff)
    db_usr = j['dbUsr']
    db_pwd = j['dbPwd']
    db_hst = j['dbHst']
    db_name = j['dbNm']
with open('./config.json') as cc:
    j = load(cc)
    usd_check = j['usdcheck']


def get_response(symbol: str, from_t: int, to_t: int, step: int):
    if step not in [1, 15, 60]:
        return b''
    client = socket(AF_INET, SOCK_STREAM)
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
            # "eod_time": '',
            "rate": 1.0
        }
    original_date = d.fromisoformat(trade_date)
    eod_date = int(d(
        original_date.year, original_date.month, (
            original_date.day + next_day),
        eod_hour, 0, 0).timestamp()
    )

    response = get_response(symbol + '.rp', eod_date, eod_date, 60)
    if len(response) == 4 or len(response) == 0:
        return{
            "symbol": symbol,
            "origin_time": trade_date,
            # "eod_time": '',
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
            # "eod_time": d.fromtimestamp(body[0]),
            "rate": (body[1] + body[4]) * digit
        }


def create_symbol_list(f_time: str, e_time: str):
    try:
        d.fromisoformat(f_time)
        d.fromisoformat(e_time)
    except Exception as e:
        print(e)
        return
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
                if "USD" == s[:3]:
                    continue
                if "USD" != s[:3] and "USD" != s[3:]:
                    try:
                        alter_s = usd_check[s[:3]].split('.')[0]
                        temp = (alter_s, t)
                    except KeyError:
                        print('create symbol list error symbol {}'.format(s))
                        continue
                else:
                    temp = (s, t)
            else:
                try:
                    alter_s = s
                    temp = (alter_s, t)
                except KeyError:
                    print('create symbol list error symbol {}'.format(s))
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
                if "USD" == s[:3]:
                    continue
                if "USD" != s[:3] and "USD" != s[3:]:
                    try:
                        alter_s = usd_check[s[:3]].split('.')[0]
                        temp = (alter_s, t)
                    except KeyError:
                        print('create symbol list error symbol {}'.format(s))
                        continue
                else:
                    temp = (s, t)
            else:
                try:
                    alter_s = s
                    temp = (alter_s, t)
                except KeyError:
                    print('create symbol list error symbol {}'.format(s))
                    continue
            if temp not in symbol_date_pair:
                symbol_date_pair.append(temp)
            else:
                continue
    except Exception as e:
        print('database error in get_valid_user function')
        con.rollback()
    finally:
        con.close()
    return symbol_date_pair
