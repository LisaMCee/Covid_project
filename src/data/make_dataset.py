# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import copy
import requests
import json
from datetime import timedelta, date, datetime
import os
from pathlib import Path

def get_latest_data():
    print('Getting latest COVID cumulative cases')
    data = requests.get('http://pomber.github.io/covid19/timeseries.json').json()
    keys_countries= data.keys() 
    df_cumulative=pd.DataFrame()
    for key in data:
        df1= pd.json_normalize(data[key])
        df1['Country']=key
        df_cumulative= df_cumulative.append(df1)
# Dataframe with daily cases per country (above is cumulatve)
    df_daily=pd.DataFrame()
    print('Getting latest COVID daily cases')
    for key in data:
        df1= pd.json_normalize(data[key])
        df1.set_index(['date'],inplace=True)
        df2=copy.deepcopy(df1)
        for col in df2.columns:
            df2[col]=df2[col]- df2[col].shift(1)
        df2['Country']=key
        df_daily= df_daily.append(df2)
    df_daily.reset_index(inplace=True)
    return df_cumulative, df_daily

def get_populations():    
    # Get population data
    url = 'https://data.un.org/_Docs/SYB/CSV/SYB63_1_202105_Population,%20Surface%20Area%20and%20Density.csv'
    df_data= pd.read_csv(url, header=1)
    #df with just country and population, convert from ('millions')
    df_pops=df_data[(df_data['Year']==2019)&(df_data['Series']==df_data.Series[0])][['Unnamed: 1','Value']]
    df_pops.columns=['Country', 'Population']
    df_pops.Population = df_pops.Population*1000000
    return df_pops

def get_PHT_df(df_pops, data_df):
#clean countrynames to be consistent across dfs
    if os.path.isfile('../data/interim/country_namechanges.json'):
        with open('../data/interim/country_namechanges.json', 'r') as fp:
            new_names = json.load(fp)
    else:
        countries_pops=pd.unique(df_pops.Country)
        countries_namechange= []
        for country in (pd.unique(data_df.Country)):
            if country not in (pd.unique(df_pops.Country)):
                countries_namechange.append(country)
        new_names={}
        for country in countries_namechange:
            to_check= country[:4]
            result = [v for v in countries_pops if to_check in v]
            if result:
                print(result)
                print(country + ': type index to keep or else F')
                x = input()
                if x == 'F':
                    new=country
                    print('no match')
                else:
                    new=result[int(x)]
                    print('{} is changed to {}'.format(country, new))
            else:
                print(country, ': no match')
                new=country
            new_names[country]= new
    #save to json file
        with open('../data/interim/country_namechanges.json', 'w') as fp:
            json.dump(new_names, fp)
    #convert the names in covid data to names in the population data
    new_names['US']='United States'
    new_names['United States of America']='United States'
    for key in new_names:
        data_df.replace(to_replace=key, value=new_names[key], inplace=True)
    data_df.date = pd.to_datetime(data_df.date)
    #new df with populations and data
    print('Calculating cases and deaths per hundred thousand population')
    data_df_pops = pd.merge(data_df, df_pops, on='Country')
    #adjust data to 'per 100 thousand of populaiton'
    data_df_PHT = copy.deepcopy(data_df_pops)
    for col in data_df.columns[1:4]:
        data_df_PHT[col]= data_df_PHT[col]*100000/(data_df_PHT['Population']).apply(pd.to_numeric, errors='coerce')
    return data_df_PHT

def get_all_datasets():
    df_pops=get_populations()
    df_cumulative, df_daily=get_latest_data()
    data_df_PHT_cumul=get_PHT_df(df_pops, df_cumulative)
    data_df_PHT_daily=get_PHT_df(df_pops, df_daily)
    return df_cumulative,df_daily, data_df_PHT_cumul, data_df_PHT_daily

def save_data(dataframe, filename, root_dir, sub_dir):
    if root_dir[-1] != "/":
                root_dir += "/"
    if sub_dir[-1] != "/":
                root_dir += sub_dir + "/"
    try:
        filepath_out = root_dir + filename + ".csv"
        Path(root_dir).mkdir(parents=True, exist_ok=True)
        print('Saving {}...'.format(filepath_out))
        dataframe.to_csv(filepath_out)
        print("Saved")
    except Exception as e:
            print(e)
            pass
