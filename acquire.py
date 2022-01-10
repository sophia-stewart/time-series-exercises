# TIME SERIES DATA ACQUISITION FUNCTIONS
import pandas as pd
import requests

def get_data(target):
    '''
    This function takes in a target specifying which dataset to retrieve from 
    https://python.zgulde.net/ It returns a dataframe of that data and creates a
    local csv file of that data. If a local csv file of the same name as the target 
    already exists, it pulls in data from that csv file instead.
    '''
    import os
    # check for local csv
    if os.path.isfile(f'{target}.csv'):
        df = pd.read_csv(f'{target}.csv', index_col=0)
        return df
    else: 
        url = 'https://python.zgulde.net/'
        p_1 = f'api/v1/{target}'
        # access url
        response = requests.get(url+p_1)
        # assign json data to variable
        data = response.json()
        # convert page 1 results to dataframe
        df = pd.DataFrame(data['payload'][f'{target}'])
        # get data from every page
        while data['payload']['page'] < data['payload']['max_page']:
            response = requests.get(url + data['payload']['next_page'])
            data = response.json()
            df = pd.concat([df, pd.DataFrame(data['payload'][f'{target}'])])
        # write df to csv
        df.to_csv(f'{target}.csv')
    # reset index, drop extra column
    return df.reset_index().drop(columns='index')

def merge_dfs(sales, stores, items):
    '''
    This function takes in the sales, stores, and items dataframes and merges them into a 
    single dataframe.
    '''
    # merge all three dfs into one
    df = sales.merge(stores, left_on='store', right_on='store_id').merge(items, left_on='item', right_on='item_id')
    return df.drop(columns=['store', 'item'])

def get_germany():
    '''
    This function takes in no arguments and returns a dataframe of the data contained in
    https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv. It then 
    writes this dataframe to a local file: germany.csv. If this file already exists locally
    the function pulls in data from the local csv.
    '''
    import os
    # check for local csv
    if os.path.isfile('germany.csv'):
        germany = pd.read_csv('germany.csv', index_col=0)
    else: 
        germany = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
        germany.to_csv('germany.csv')
    return germany