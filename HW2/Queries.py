import pandas as pd

def query1():
    emp = pd.read_csv("emp.csv")
    emp = emp.sort_values(by=['DEPTNO', 'EMPNO'])
    emp = emp[['DEPTNO', 'EMPNO', 'ENAME', 'SAL']]

    acc_sal_col = []
    for dept in emp['DEPTNO'].unique():
        each_dept = emp[emp['DEPTNO']==dept]
        size = len(each_dept.index)
        i = 0

        while i<size:
            acc_sal = 0
            acc_sal += each_dept.iloc[i]['SAL']
            if i-1>=0: acc_sal += each_dept.iloc[i-1]['SAL']
            if i+1<size: acc_sal += each_dept.iloc[i+1]['SAL']

            acc_sal_col.append(acc_sal)
            i += 1
        
    emp['ACC_SAL'] = acc_sal_col

    print(emp.to_string(index=False))
            
    return

def query2():
    stock = pd.read_csv("stock_price.csv", parse_dates=['price_date'])
    stock = stock.sort_values(by=['company', 'price_date'])

    result = []
    for comp in stock['company'].unique():
        each_comp = stock[stock['company']==comp]
        size = len(each_comp.index)
        i = 0

        while i<size-2:
            current_price = each_comp.iloc[i]['price']
            next_price = each_comp.iloc[i+1]['price']
            nnext_price = each_comp.iloc[i+2]['price']
            
            if current_price <= 50 and next_price <= current_price*0.9 and nnext_price >= next_price*1.05:
                result += each_comp.iloc[i:i+3].values.tolist()
                i += 3          # Default : "AFTER MATCH SKIP PAST LAST ROW"
            else:
                i += 1
            
    print(pd.DataFrame(result, columns=['company', 'price_date', 'price']).to_string(index=False))

    return

query1()
query2()