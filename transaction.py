import MySQLdb as mdb
from json import load
from numpy import array,unique,dtype
from pandas import DataFrame, to_datetime, to_numeric, read_sql_query
from datetime import datetime as d


with open('./setting.json') as ff:
    j = load(ff)
    db_usr = j['dbUsr']
    db_pwd = j['dbPwd']
    db_hst = j['dbHst']
    db_name = j['dbNm']
with open('./config.json') as cc:
    j = load(cc)
    cz = j['CZ']
    salesList = j['sales']
    usdCheck = j['usdcheck']
    indices = j['indices']


class Sales_volume:
    def __init__(self, sale: int, from_time: str, to_time: str, open_trans=DataFrame(array([])), close_trans=DataFrame(array([]))):
        """ticket : <class 'numpy.int64'>
            volume: <class 'numpy.int64'>
            symbol: <class 'str'>
            opentime: <class 'datetime.date'>
            closetime: <class 'datetime.date'>
            sales: <class ''>
        """
        self.sale = sale
        self.begin_time = from_time
        self.end_time = to_time
        self.set_by_pair(open_df=open_trans, close_df=close_trans)
        self.set_pair_time()

    def get_total_pair(self): return self.total_gp_pair

    def get_pair_time_df(self): return self.pair_time_df

    def set_by_pair(self, open_df, close_df):
        open_group = open_df.groupby(['symbol', 'time']).groups
        close_group = close_df.groupby(['symbol', 'time']).groups
        keys = []
        total_pair = {}
        for k in open_group.keys():
            keys.append(k[0] + '@' + str(k[1]))
        for k in close_group.keys():
            keys.append(k[0] + '@' + str(k[1]))
        keys = unique(keys)
        for k in keys:
            new_key = (k.split('@')[0], to_datetime(k.split('@')[1]))
            open_ele = close_ele = []
            ticket_list = []
            try:
                temp_ele = open_group[new_key]
            except KeyError:
                temp_ele = []
            finally:
                open_ele = temp_ele
            try:
                temp_ele = close_group[new_key]
            except:
                temp_ele = []
            finally:
                close_ele = temp_ele
            if 0 != len(open_ele):
                for i in open_ele:
                    row = open_df.loc[i]
                    ticket_list.append(
                        (row['ticket'], row['volume'], row['login'], 'open'))
            if 0 != len(close_ele):
                for i in close_ele:
                    row = close_df.loc[i]
                    ticket_list.append(
                        (row['ticket'], row['volume'], row['login'], 'close'))
            temp_df = DataFrame(array(ticket_list), columns=[
                                   'ticket', 'volume', 'login', 'status'])
            temp_df['volume'] = to_numeric(temp_df['volume'])
            try:
                total_pair[(new_key[0], str(new_key[1]).split(' ')[0])] = {
                    'total_volume': temp_df['volume'].sum() * (cz[new_key[0]]/100),
                    # "cz": cz[new_key[0],
                    'detail': temp_df
                }
            except KeyError:
                if 6 == len(new_key[0]):
                    total_pair[(new_key[0], str(new_key[1]).split(' ')[0])] = {
                        'total_volume': temp_df['volume'].sum() * 1000,
                        # "cz": cz[new_key[0],
                        'detail': temp_df
                    }
                else:
                    print("Symbol({}) is not in the Contract Size list".format(new_key[0]))
                    continue
        self.total_gp_pair = total_pair

    def set_pair_time(self):
        gp_pair = self.total_gp_pair
        general_list = []
        for k in gp_pair.keys():
            pair = k[0]
            tr_t = k[1]
            if 6 == len(pair):
                base = pair[:3]
                mapping_pair = ""
                if 'USD' == base:
                    mapping_pair = "USD.rp"
                else:
                    mapping_pair = usdCheck[base]
                temp_list_ele = [pair, mapping_pair, tr_t,
                                 gp_pair[k]['total_volume'], 0.0, 0.0]
                general_list.append(temp_list_ele)
            else:
                mapping_pair = indices[pair] + '.rp'
                temp_list_ele = [pair, mapping_pair, tr_t,
                                 gp_pair[k]['total_volume'], 0.0, 0.0]
                general_list.append(temp_list_ele)

        new_general_list = DataFrame(array(general_list), columns=(
            'symbol', 'alter_symbol', 'time', 'TVolume', 'rate', 'price'))
        new_general_list['TVolume'] = to_numeric(
            new_general_list['TVolume'])
        new_general_list['price'] = to_numeric(new_general_list['price'])
        new_general_list['rate'] = to_numeric(new_general_list['rate'])

        self.pair_time_df = new_general_list

    def set_by_base(self, rates: type(DataFrame())):
        if 0 == rates.size:
            return {}
        temp_dir = {}
        for k in self.pair_time_df.iterrows():
            p = k[1][1]
            t = k[1][2]
            if "USD.rp" == p:
                r = 1.0
                self.pair_time_df.iloc[k[0], 4] = r
                self.pair_time_df.iloc[k[0], 5] = k[1][3] * r
            else:
                try:
                    r = rates[(rates['pair'] == p) & (rates['time'] == t)]['finalRate']._values[0]
                    self.pair_time_df.iloc[k[0], 4] = r
                    self.pair_time_df.iloc[k[0], 5] = k[1][3] * r
                except IndexError:
                    print("no eod rate for {} in {}".format(p, t))
                    continue

        for r in self.pair_time_df.iterrows():
            pair = r[1][0]
            if 6 == len(pair):
                base = r[1][0][:3]
            else:
                base = pair
            try:
                temp_dir[base]
            except KeyError:
                temp_dir[base] = {
                    'volume': 0.0,
                    'price': 0.0
                }
            finally:
                temp_dir[base]['volume'] += r[1][3]
                temp_dir[base]['price'] += r[1][5]
        # print(temp_dir)
        final_list = []
        for k in temp_dir.keys():
            final_list.append([k, temp_dir[k]['volume'], temp_dir[k]['price']])
        final_df = DataFrame(array(final_list), columns=(
            'Base Cr', 'Base Vol', 'Vol in USD'))
        final_df['Base Vol'] = to_numeric(final_df['Base Vol'])
        final_df['Vol in USD'] = to_numeric(final_df['Vol in USD'])
        return final_df


def get_valid_user(sales_list: list = salesList):
    now = d.now().timestamp()
    con = mdb.connect(db_hst, db_usr, db_pwd, db_name)

    """ the way to plolute sql result to dataframe
    try:
        logins = read_sql_query(
            "SELECT u.login, u.status FROM MT4_USERS as u where u.status in ('%(sales)s','1002','1003','1004','1005','1006','1007','1009','1010','2001','2002','3001','3002','100') and u.group in ('U-RZST0000001','U-RZCT0000001','U-RZUT0000001','E-RZST0000001','E-RZUT0000001','URZCT0000R01031','URZCTS7C35R031','URZTIB1','URZST0000R03001');", con,params={'sales':1001})
        return logins
    except Exception as e:
        print('database error in get_valid_user function, {}'.format(e))
        con.rollback()
        return DataFrame(data=array([]))
    finally:
        print('get_valid_user function run for {} seconds'.format(
            d.now().timestamp() - now))
        con.close()
    """

    cur = con.cursor()
    try:
        cur.execute("SELECT u.login, u.status FROM MT4_USERS as u where u.status in ('1001','1002','1003','1004','1005','1006','1007','1009','1010','2001','2002','3001','3002','100') and u.group in ('U-RZST0000001','U-RZCT0000001','U-RZUT0000001','E-RZST0000001','E-RZUT0000001','URZCT0000R01031','URZCTS7C35R031','URZTIB1','URZST0000R03001');")
        return DataFrame(data=array(cur.fetchall()), columns=('login', 'status'), dtype=(dtype('i4'), dtype('i4')))
    except Exception as e:
        print('database error in get_valid_user function, {}'.format(e))
        con.rollback()
        return DataFrame(data=array([]))
    finally:
        print('get_valid_user function run for {} seconds'.format(
            d.now().timestamp() - now))
        con.close()


async def get_transaction(fromT: str, toT: str):
    con = mdb.connect(db_hst, db_usr, db_pwd, db_name)
    open_transactions = DataFrame(array([]))
    close_transactions = DataFrame(array([]))

    try:
        open_transactions = read_sql_query(
            "select t.ticket,t.login,t.volume,substring_index(t.symbol,'.',1) as symbol,date(t.open_time) as time,u.status as sales from MT4_TRADES as t join (SELECT u.login, u.status FROM MT4_USERS as u where u.status in ('1001','1002','1003','1004','1005','1006','1007','1009','1010','2001','2002','3001','3002','100') and u.group in ('U-RZST0000001','U-RZCT0000001','U-RZUT0000001','E-RZST0000001','E-RZUT0000001','URZCT0000R01031','URZCTS7C35R031','URZTIB1','URZST0000R03001')) as u on u.login = t.login where t.open_time between date(%(from)s) and date(%(to)s) and t.conv_rate1 != 0.0 and t.symbol != ''order by u.status ASC;", con, params={'from': fromT, 'to': toT})

        open_transactions['ticket'] = to_numeric(
            open_transactions['ticket'])
        open_transactions['login'] = to_numeric(open_transactions['login'])
        open_transactions['volume'] = to_numeric(
            open_transactions['volume'])
        open_transactions['time'] = to_datetime(open_transactions['time'])
        open_transactions['sales'] = to_numeric(open_transactions['sales'])

        close_transactions = read_sql_query(
            "select t.ticket,t.login,t.volume,substring_index(t.symbol,'.',1) as symbol,date(t.close_time) as time,u.status as sales from MT4_TRADES as t join (SELECT u.login, u.status FROM MT4_USERS as u where u.status in ('1001','1002','1003','1004','1005','1006','1007','1009','1010','2001','2002','3001','3002','100') and u.group in ('U-RZST0000001','U-RZCT0000001','U-RZUT0000001','E-RZST0000001','E-RZUT0000001','URZCT0000R01031','URZCTS7C35R031','URZTIB1','URZST0000R03001')) as u on u.login = t.login where t.close_time between date(%(from)s) and date(%(to)s) and t.conv_rate1 != 0.0 and t.symbol != ''order by u.status ASC;", con, params={'from': fromT, 'to': toT})

        close_transactions['ticket'] = to_numeric(
            close_transactions['ticket'])
        close_transactions['login'] = to_numeric(
            close_transactions['login'])
        close_transactions['volume'] = to_numeric(
            close_transactions['volume'])
        close_transactions['time'] = to_datetime(close_transactions['time'])
        close_transactions['sales'] = to_numeric(
            close_transactions['sales'])

    except Exception as e:
        print('database error in get_transaction function, {}'.format(e))
        con.rollback()
        open_transactions = DataFrame(array([]))
        close_transactions = DataFrame(array([]))
    finally:
        con.close()
        return {'open': open_transactions, 'close': close_transactions}


async def create_sales_obj(fromt_t: str, to_t: str, sales_list=salesList):
    now = d.now().timestamp()
    sale_obj_list = []
    try:
        d.fromisoformat(fromt_t)
        d.fromisoformat(to_t)
    except Exception as e:
        print(e)
        return
    total_df = await get_transaction(fromt_t, to_t)
    open_total_df = total_df['open']
    close_total_df = total_df['close']

    for s in sales_list:
        temp_open_df = DataFrame(
            data=(open_total_df.loc[open_total_df['sales'] == int(s)].iloc[:, :5]))
        temp_close_df = DataFrame(
            data=(close_total_df.loc[close_total_df['sales'] == int(s)].iloc[:, :5]))

        if 0 != temp_open_df.size and 0 != temp_close_df.size:
            sale_obj_list.append(Sales_volume(
                int(s), fromt_t, to_t, temp_open_df, temp_close_df))
        elif 0 != temp_open_df.size and 0 == temp_close_df.size:
            sale_obj_list.append(Sales_volume(
                int(s), fromt_t, to_t, open_trans=temp_open_df))
        elif 0 != temp_open_df.size and 0 == temp_close_df.size:
            sale_obj_list.append(Sales_volume(
                int(s), fromt_t, to_t, close_trans=temp_close_df))
        else:
            continue
    print('create_sales_obj function run for {} seconds'.format(
        d.now().timestamp() - now))

    return array(sale_obj_list)
