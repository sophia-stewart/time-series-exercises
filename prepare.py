# FUNCTIONS FOR DATA PREPARATION

import pandas as pd

def prepare_sales(sales):
    '''
    This function takes in the sales dataframe and returns a prepared version
    of the sales dataframe.
    '''
    sales.sale_date = pd.to_datetime(sales.sale_date)
    sales = sales.set_index('sale_date').sort_index()
    sales['month'] = sales.index.month_name()
    sales['day_of_week'] = sales.index.day_name()
    sales['sales_total'] = sales.sale_amount * sales.item_price
    return sales

def prepare_germany(germany):
    germany.Date = pd.to_datetime(germany.Date)
    germany = germany.set_index('Date').sort_index()
    germany['month'] = germany.index.month_name()
    germany['year'] = germany.index.year
    germany = germany.fillna(0)
    return germany