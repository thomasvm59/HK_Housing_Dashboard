import datetime
import pandas as pd
import streamlit as st
import numpy as np
import requests
import json
from web_scrapping import *
from database import *
from geopy.distance import geodesic
OFFICE_COORD = (22.28492, 114.15951)

# Define provinces and areas
provinces = ['HK Island', 'Kowloon', 'New Territories', 'Islands']
hk_island_areas = [
    "Kennedy Town", "Sai Ying Pun", "Shek Tong Tsui", "Central", "Sheung Wan",
    "Mid-Level", "West-Level", "Wan Chai", "Admiralty", "Causeway Bay", "Happy Valley",
    "Tin Hau", "Tai Hang", "North Point", "Fortress Hill", "Mid Quarry Bay", "TaiKoo",
    "Sai Wan Ho", "Shau Kei Wan", "Heng Fa Chuen", "Chai Wan", "Siu Sai Wan", "Shek O",
    "Aberdeen", "Ap Lei Chau", "Wong Chuk Hang", "Island South"
]
kowloon_areas = [
    "Lam Tin", "Yau Tong", "Kwun Tong", "Ngau Tau Kok", "Kowloon Bay", "Ngau Chi Wan",
    "Diamond Hill", "Lok Fu", "To Kwa Wan", "Kowloon City", "Kai Tak", "San Po Kong",
    "Wong Tai Sin", "Kowloon Tong", "Ho Man Tin", "Yau Yat Tsuen", "Sham Shui Po",
    "Shek Kip Mei", "Nam Cheong", "Lai Chi Kok", "Cheung Sha Wan", "Mei Foo", "Lai King",
    "Tai Kok Tsui", "Olympic", "Kowloon Station", "Prince Edward", "Mong Kok", "Yau Ma Tei",
    "Jordan", "Tsim Sha Tsui", "Hung Hom", "Whampoa"
]
new_t_areas = [
    "Sai Kung", "Clear Water Bay", "Tseung Kwan O", "Ma On Shan", "Sha Tin", "Tai Wai",
    "Fotan", "Tai Po", "Tai Wo", "Pak Shek Kok", "Fan Ling", "Sheung Shui", "Yuen Long",
    "Hung Shui Kiu", "Tin Shui Wai", "Tuen Mun", "Tuen Mun Castle Peak Road", "Sham Tseng",
    "Tsuen Wan", "Tai Wo Hau", "Kwai Chung", "Kwai Fong", "Tsing Yi", "Sha Tau Kok"
]
island_areas = [
    "Ma Wan", "Discovery Bay", "Tung Chung", "South Lantau Island", "Tai O", "Peng Chau",
    "Lamma Island", "Cheung Chau", "Other Islands"
]
neighborhood_to_district = {
    'Causeway Bay': 'Wan Chai',
    'Central': 'Central and Western',
    'Cheung Sha Wan': 'Sham Shui Po',
    'Clear Water Bay': 'Eastern',
    'Fortress Hill': 'Eastern',
    'Fotan': 'Sha Tin',
    'Happy Valley': 'Wan Chai',
    'Ho Man Tin': 'Kowloon City',
    'Hung Hom': 'Yau Tsim Mong',
    'Hung Shui Kiu': 'Yuen Long',
    'Island South': 'Southern',
    'Jordan': 'Yau Tsim Mong',
    'Kai Tak': 'Kowloon City',
    'Kowloon City': 'Kowloon City',
    'Kowloon Station': 'Yau Tsim Mong',
    'Kwai Chung': 'Kwai Tsing',
    'Kwai Fong': 'Kwai Tsing',
    'Kwun Tong': 'Kwun Tong',
    'Lai Chi Kok': 'Sham Shui Po',
    'Lai King': 'Tsuen Wan',
    'Ma On Shan': 'Sha Tin',
    'Ma Wan': 'Islands',
    'Mei Foo': 'Sham Shui Po',
    'Mong Kok': 'Yau Tsim Mong',
    'Nam Cheong': 'Sham Shui Po',
    'Ngau Tau Kok': 'Kwun Tong',
    'None': None,  # This might represent an unknown or missing value
    'North Point': 'Eastern',
    'Olympic': 'Kowloon City',
    'Pak Shek Kok': 'Sha Tin',
    'Peng Chau': 'Islands',
    'Prince Edward': 'Yau Tsim Mong',
    'Sai Kung': 'Sai Kung',
    'Sai Wan Ho': 'Eastern',
    'Sai Ying Pun': 'Central and Western',
    'Sha Tin': 'Sha Tin',
    'Sham Shui Po': 'Sham Shui Po',
    'Sham Tseng': 'Tsuen Wan',
    'Shau Kei Wan': 'Eastern',
    'Shek Tong Tsui': 'Central and Western',
    'Sheung Shui': 'North',
    'Sheung Wan': 'Central and Western',
    'South Lantau Island': 'Islands',
    'Tai Kok Tsui': 'Yau Tsim Mong',
    'Tai Po': 'Tai Po',
    'Tai Wai': 'Sha Tin',
    'Tin Hau': 'Wan Chai',
    'Tin Shui Wai': 'Yuen Long',
    'To Kwa Wan': 'Kowloon City',
    'Tseung Kwan O': 'Sai Kung',
    'Tsim Sha Tsui': 'Yau Tsim Mong',
    'Tsing Yi': 'Kwai Tsing',
    'Tsuen Wan': 'Tsuen Wan',
    'Tuen Mun': 'Tuen Mun',
    'Tung Chung': 'Islands',
    'Wan Chai': 'Wan Chai',
    'Yau Ma Tei': 'Yau Tsim Mong',
    'Yau Tong': 'Kwun Tong',
    'Yuen Long': 'Yuen Long'
}

# Combine areas
areas_mapping = {
    'HK Island': hk_island_areas,
    'Kowloon': kowloon_areas,
    'New Territories': new_t_areas,
    'Islands': island_areas
}
DISPLAY_COLUMNS=['province', 'area', 'lease_price','floor_size', 'unit_price',
        'historical_average','unit_price_vs_histo', 'floor_zone', 'number_of_rooms', 'number_of_bathrooms', 'num_units',
       'date_published', 'distance_to_office_km', 'address','name', 'url',
       'url_history','url_transit'
       ]

COLUMNS_DETAILS_DB = ['date_published', 'property_number',
       'price', 'floor_size', 'floor_size_unit', 'unit_price' ,
       'latitude', 'longitude', 'address', 'name',
        'number_of_rooms', 'number_of_bathrooms', 'num_units', 'floor_zone',
        'property_type', 'description', 'url', 'url_history',
       ]


# Functions to extract province and area
def extract_province(address):
    for province in provinces:
        if province in address:
            return province
    return None

def extract_area(address, province):
    if province and province in areas_mapping:
        for area in areas_mapping[province]:
            if area in address:
                return area
    return None

@st.cache_data
def update_database(now_ts):
    property_numbers_list = list_of_properties_scrapping()
    df_listing_old = load_data_listing_db()
    old_number_list = list(df_listing_old['property_number'])
    new_listing = [num for num in property_numbers_list if num not in old_number_list]
    df_listing_new = get_properties_dataframe_parallel(new_listing)
    df_listing_new = df_listing_new.dropna(subset=['property_number']).reset_index()
    df_listing_new=df_listing_new[COLUMNS_DETAILS_DB]
    df_listing_new['property_number']=df_listing_new['property_number'].astype(int)
    df_concat_listing = pd.concat([df_listing_old,df_listing_new.astype(str)]).reset_index(drop=True)
    df_concat_listing = df_concat_listing[lambda x : x.date_published!='Not available'].sort_values('date_published',ascending=False)
    save_list_properties_db(property_numbers_list, now_ts)
    save_data_listing_db(df_concat_listing[lambda x : x.property_number.isin(property_numbers_list)])

    
@st.cache_data
def load_data(now_ts, update_db=False):
    if update_db:
        update_database(now_ts)
    df_listing = load_data_listing_db()
    df_history = load_data_history_db()
    update_dt = datetime.datetime.now(tz=datetime.timezone.utc)
    fx_rates = transform_fx_rates(get_fx_rates())
    coordinates_map=coordinates_map_districts()
    df_listing = process_data_listing(df_listing)
    df_history = process_data_history(df_history)
    return df_listing, df_history, fx_rates, coordinates_map, update_dt

def process_data_listing(df):
    df = df[lambda x : x.property_type!='Office']
    df = df.replace('nan', np.nan)
    df = df.dropna(subset=['date_published'])
    df = fix_coordinates(df)
    df['distance_to_office_km']=distance_to_office(df)
    df['url_transit']=transit_url(df)
    df['province'] = df['address'].apply(extract_province)
    df['area'] = df.apply(lambda row: extract_area(row['address'], row['province']), axis=1)
    df['area_district'] = df['area'].apply(lambda x: neighborhood_to_district.get(x, 'Unknown'))
    df["lease_price"] = df["price"].str.extract(r"HKD\$([\d,]+)").replace(",", "", regex=True).astype(int)
    df['floor_size'] = pd.to_numeric(df['floor_size'], errors='coerce')  # Convert to numeric, set 'Not Available' as NaN
    df['unit_price'] = df['lease_price'].astype(int) / df['floor_size'] 
    return df

def process_data_history(df):
    df = df.replace('nan', np.nan)
    df = fix_coordinates(df)
    df['province'] = df['address'].apply(extract_province)
    df['area'] = df.apply(lambda row: extract_area(row['address'], row['province']), axis=1)
    df['area_district'] = df['area'].apply(lambda x: neighborhood_to_district.get(x, 'Unknown'))
    df=df.rename(columns={'Size (ft²)':'floor_size',
                 'Unit Price (HKD/ft²)': 'unit_price',
                 'Leased Price (HKD)' : 'lease_price'
                 })
    df['floor_size'] = pd.to_numeric(df['floor_size'], errors='coerce')
    df['unit_price'] = df['lease_price'].astype(int) / df['floor_size'] 
    df['lease_date']=pd.to_datetime(df['Lease Date'])
    return df

def fix_coordinates(df): # TO MODIFY TO OVERWRITE COORD FROM ADDRESS
    df = df[lambda x : x.latitude!='Not available']
    df = df[lambda x : x.latitude.astype(float)>10]
    df = df[lambda x : x.latitude.astype(float)<30]
    return df
    

def get_fx_rates():
    # Free Exchange Rate API (replace with your API key if needed)
    url = "https://api.exchangerate-api.com/v4/latest/USD"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        rates = data.get('rates', {})
        currencies = ["AUD", "USD", "HKD", "KRW", "EUR"]
        exchange_rates = {currency: rates.get(currency) for currency in currencies}
        return exchange_rates
    else:
        print(f"Error: {response.status_code} - {response.text}")
        
def coordinates_map_districts():
    with open("data_coordinates_map.json", "r") as f:
        coordinates_map = json.load(f)
    return coordinates_map

def transform_fx_rates(fx_rates_dict):
    fx_rates_dict['KRW(K)']=fx_rates_dict['KRW']/1000
    del fx_rates_dict['KRW']
    return fx_rates_dict

def distance_to_office(df):
    return [geodesic(OFFICE_COORD, (float(df.iloc[i]['latitude']), float(df.iloc[i]['longitude']))).kilometers for i in range(len(df)) ]

def transit_url(df):
    return [citymapper_url_from_coords(OFFICE_COORD, (float(df.iloc[i]['latitude']), float(df.iloc[i]['longitude']))) for i in range(len(df)) ]

def citymapper_url_from_coords(start_coords, end_coords):
    """
    Generate a Citymapper directions URL with the given start and end coordinates.
    
    :param start_coords: Tuple (lat, lon) for the starting point.
    :param end_coords: Tuple (lat, lon) for the destination.
    :return: Citymapper URL as a string.
    """
    base_url = "https://citymapper.com/directions?"
    url = f"{base_url}startcoord={start_coords[0]}%2C{start_coords[1]}&endcoord={end_coords[0]}%2C{end_coords[1]}"
    return url

