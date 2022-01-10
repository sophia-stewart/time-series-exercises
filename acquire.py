# TIME SERIES DATA ACQUISITION FUNCTIONS

def get_data(target):
    url = 'https://python.zgulde.net/'
    p_1 = f'api/v1/{target}'
    # access url
    response = requests.get(url+p_1)
    # assign json data to variable
    data = response.json()
    # convert page 1 results to dataframe
    df = pd.DataFrame(data['payload'][f'{target}'])
    while data['payload']['page'] < data['payload']['max_page']:
        response = requests.get(url + data['payload']['next_page'])
        data = response.json()
        df = pd.concat([df, pd.DataFrame(data['payload'][f'{target}'])])
    return df.reset_index().drop(columns='index')

def get_data_csv(target):
    url = 'https://python.zgulde.net/'
    p_1 = f'api/v1/{target}'
    # access url
    response = requests.get(url+p_1)
    # assign json data to variable
    data = response.json()
    # convert page 1 results to dataframe
    df = pd.DataFrame(data['payload'][f'{target}'])
    while data['payload']['page'] < data['payload']['max_page']:
        response = requests.get(url + data['payload']['next_page'])
        data = response.json()
        df = pd.concat([df, pd.DataFrame(data['payload'][f'{target}'])])
    df.to_csv(f'{target}.csv')
    return df.reset_index().drop(columns='index')

def merge_dfs(sales, stores, items):
    df = sales.merge(stores, left_on='store', right_on='store_id').merge(items, left_on='item', right_on='item_id')
    return df

