import cal as cal

def main():
    sumary = cal.calculation('2018-07-09','2018-07-15')
    for k in sumary.keys():
        print(k)
        print(sumary[k])
        print('\n')
  
if __name__ == '__main__':
    main()