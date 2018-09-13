import cal as cal
import sys
import pandas as pd
# import matplotlib.pyplot as plt

def main():
    sumary = cal.calculation(sys.argv[1],sys.argv[2])
    outcome = pd.ExcelWriter('./outcome/' +sys.argv[1]+ '~' +sys.argv[2]+'.xlsx',engine='xlsxwriter')
    for k in sumary.keys():
        print(k)
        rs = sumary[k]['summary'].shape[0]
        sumary[k]['summary'].to_excel(outcome,sheet_name=str(k))
        # outcome_worksheet = outcome.sheets[str(k)]
        # for row_num,v in sumary[k]['detail'].iterrows():
        #     outcome_worksheet.write(row_num+rs+2,)
        sumary[k]['detail'].to_excel(outcome, sheet_name= str(k),startrow=(rs+2))
    outcome.save()

if __name__ == '__main__':
    main()