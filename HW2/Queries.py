import pandas as pd


def query1():
    emp = pd.read_csv("emp.csv")
    emp = emp.sort_values(by=['DEPTNO', 'EMPNO'])
    emp['ACC_SAL'] = emp.groupby('DEPTNO')['SAL'].rolling(window=3, min_periods=1, center=True).sum().reset_index(level=0, drop=True)

    print(emp[['DEPTNO', 'EMPNO', 'ENAME', 'SAL', 'ACC_SAL']].to_string(index=False))

    return

def query2():
    stock = pd.read_csv("stock_price.csv", parse_dates=['price_date'])
    stock = stock.sort_values(by=['company', 'price_date'])

    result = []
    for comp in stock['company'].unique():
        each_comp = stock[stock['company']==comp]
        
        for idx in range(len(each_comp)-2):
            current_price = each_comp.iloc[idx]['price']
            next_price = each_comp.iloc[idx+1]['price']
            nnext_price = each_comp.iloc[idx+2]['price']
            
            if current_price <= 50 and next_price <= current_price*0.9 and nnext_price >= next_price*1.05:
                result += each_comp.iloc[idx:idx+3].values.tolist()
    
    print(pd.DataFrame(result, columns=['company', 'price_date', 'price']).to_string(index=False))


query1()
query2()