import requests
import pandas as pd
import os

def new_retail_data(base_url='https://python.zgulde.net'):
    '''
    This function acquires new retail data, returns three dataframes, and saves those dataframes to .csv files.
    '''
    # Acquiring items data
    response = requests.get('https://python.zgulde.net/api/v1/items')
    data = response.json()
    df = pd.DataFrame(data['payload']['items'])
    while data['payload']['next_page'] != None:
        response = requests.get(base_url + data['payload']['next_page'])
        data = response.json()
        df = pd.concat([df, pd.DataFrame(data['payload']['items'])]).reset_index(drop=True)
    items_df = df.copy()
    print("Items data acquired...")

    # Acquiring stores data
    response = requests.get('https://python.zgulde.net/api/v1/stores')
    data = response.json()
    df = pd.DataFrame(data['payload']['stores'])
    stores_df = df.copy()
    print("Stores data acquired...")

    # Acquiring sales data
    response = requests.get('https://python.zgulde.net/api/v1/sales')
    data = response.json()
    df = pd.DataFrame(data['payload']['sales'])
    while data['payload']['next_page'] != None:
        response = requests.get(base_url + data['payload']['next_page'])
        data = response.json()
        df = pd.concat([df, pd.DataFrame(data['payload']['sales'])]).reset_index(drop=True)
    sales_df = df.copy()
    print("Sales data acquired")

    # Saving new data to .csv files
    items_df.to_csv("items.csv", index=False)
    stores_df.to_csv("stores.csv", index=False)
    sales_df.to_csv("sales.csv", index=False)
    print("Saving data to .csv files")

    return items_df, stores_df, sales_df

def get_store_data():
    '''
    This function reads in retail data from the website if there are no csv files to pull from
    '''
    # Checks if .csv files are present. If any are missing, will acquire new data for all three datasets
    if (os.path.isfile('items.csv') == False) or (os.path.isfile('sales.csv') == False) or (os.path.isfile('stores.csv') == False):
        print("Data is not cached. Acquiring new data...")
        items_df, stores_df, sales_df = new_retail_data()
    else:
        print("Data is cached. Reading from .csv files")
        items_df = pd.read_csv('items.csv')
        print("Items data acquired...")
        stores_df = pd.read_csv('stores.csv')
        print("Stores data acquired...")
        sales_df = pd.read_csv('sales.csv')
        print("Sales data acquired...")

    combined_df = sales_df.merge(items_df, how='left', left_on='item', right_on='item_id').drop(columns=['item'])
    combined_df = combined_df.merge(stores_df, how='left', left_on='store', right_on='store_id').drop(columns=['store'])
    print("Acquisition complete")
    return combined_df

def new_power_data():
    opsd = pd.read_csv("https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv")
    opsd = opsd.fillna(0)
    print("Saving data to .csv file")
    opsd.to_csv('opsd_germany_daily_data.csv', index=False)
    return opsd

def get_power_data():
    if os.path.isfile('opsd_germany_daily_data.csv') == False:
        print("Data is not cached. Acquiring new power data.")
        opsd = new_power_data()
    else:
        print("Data is cached. Reading data from .csv file.")
        opsd = pd.read_csv('opsd_germany_daily_data.csv')
    print("Acquisition complete")
    return opsd