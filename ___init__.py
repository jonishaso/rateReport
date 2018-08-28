import json
import pandas as pd
import numpy as np
import datetime as d
import eod_rate as er
import transaction as tr

def main():
    rates = er.rate_ready_to_cal('2018-07-09', '2018-07-15')
    rates.to_csv('./out.csv')
    
    rates = pd.read_csv('./out.csv')
    rates['finalRate'] = pd.to_numeric(rates['finalRate'])
    print(rates)
    ll = tr.create_sales_obj('2018-07-09','2018-07-15',sales_list=['1007'])
    for i in ll:
        print(i.sale)
        print(i.set_by_base(rates))
        print('\n')
        print(i.get_pair_time_df())
        print('\n')
        tp = i.get_total_pair()
        for k in tp.keys():
            print(k)
            print('\n')
            print(tp[k]['detail'])
            print('\n')
    
  
if __name__ == '__main__':
    main()