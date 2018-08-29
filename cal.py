from transaction import create_sales_obj
from eod_rate import create_symbol_list, rate_at_eod
from pandas import DataFrame as DF
from pandas import to_numeric
import numpy as np
from numpy import array as nparray
from datetime import datetime as d
from json import load
from asyncio import get_event_loop,ensure_future,wait


with open('./config.json') as cc:
    salesList = load(cc)['sales']

def data_ready(from_t:str, to_t:str,sales_list=salesList):
    now = d.now().timestamp()
    rate_result = {}
    sale_reault = nparray([])
    eod_symbol_list = create_symbol_list(from_t,to_t)
    loop = get_event_loop()
    asnyc_eod_tasks_list = [ensure_future(rate_at_eod(
        pair[0], pair[1], 0, 23)) for pair in eod_symbol_list]
    ts_2 = ensure_future(create_sales_obj(from_t,to_t,sales_list))
    asnyc_eod_tasks_list.append(ts_2)
    loop.run_until_complete(wait(asnyc_eod_tasks_list))
    for task in asnyc_eod_tasks_list:
        r = task.result()
        if type(r).__name__ == 'dict':
            try:
                rate_result[(r['symbol'], r['origin_time'])] = r['rate']
            except KeyError:
                print('error rate')
        else:
            sale_reault = r
    temp_list = []
    for k in rate_result.keys():
        init_rate = rate_result[k]
        final_rate = 0.0
        if "USD" == k[0][:3]:
            final_rate = 1/init_rate
        else:
            final_rate = init_rate
        temp_list.append([(k[0] + '.rp'), k[1], init_rate, final_rate])
    final_rate_df = DF(nparray(temp_list),columns=('pair','time','fetchRate','finalRate'))
    final_rate_df['fetchRate'] = to_numeric(final_rate_df['fetchRate'])
    final_rate_df['finalRate'] = to_numeric(final_rate_df['finalRate'])
    print("data_ready function run for {} seconds".format(d.now().timestamp() - now))
    return {
        "rates":final_rate_df,
        "sales":sale_reault
    }  
    
def calculation(from_t: str, to_t: str, sales_list=salesList):
    raw_data = data_ready('2018-07-09', '2018-07-15',sales_list)
    result = {}
    for i in raw_data['sales']:
        result[i.sale] = i.set_by_base(raw_data['rates'])
    return result
    