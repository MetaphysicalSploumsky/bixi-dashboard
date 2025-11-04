#!/usr/bin/env python3
import requests
import datetime as dt
import polars as pl
from typing import Tuple
import os

# fetch.py
# fetches data, updates the csv file
def get_coords (station_info : dict) -> Tuple[float, float] | None:
    try:
        lat = station_info['lat']
        lon = station_info['lon']
        if lat != 0 and lon != 0:
            return (lat, lon)
        else: return None
        
    except KeyError as e:
        print(f"KeyError: {e} - The key was not found in the dictionary with id: {station_info['station_id']}.")
        
def get_name (station_info : dict) -> str | None:
    try:
        name = station_info['name']
        return name
    except KeyError as e:
        print(f"KeyError: {e} - The key was not found in the dictionary with id: {station_info['station_id']}.")
        
def get_capacity (station_info : dict) -> int | None: 
    try:
        cap = station_info['capacity']
        return cap
    except KeyError as e:
        print(f"KeyError: {e} - The key was not found in the dictionary with id: {station_info['station_id']}.")
        
def get_num_bikes_available (station_status : dict) -> int | None: 
    try:
        num = station_status['num_bikes_available']
        return num
    except KeyError as e:
        print(f"KeyError: {e} - The key was not found in the dictionary with id: {station_status['station_id']}.")
        
def get_num_docks_available (station_status : dict) -> int | None:
    try:
        num = station_status['num_docks_available']
        return num
    except KeyError as e:
        print(f"KeyError: {e} - The key was not found in the dictionary with id: {station_status['station_id']}.")

def is_functional (station_status : dict) -> bool | None:
    try:
        is_functional = station_status['is_installed'] and station_status['is_renting'] and station_status['is_returning']
        return is_functional
    except KeyError as e:
        print(f"KeyError: {e} - The key was not found in the dictionary with id: {station_status['station_id']}.")
    
def main():
    station_info_url = 'https://gbfs.velobixi.com/gbfs/2-2/en/station_information.json'
    info_response = requests.get(station_info_url)
    stations_info_list = info_response.json()['data']['stations']
    info_update_time = info_response.json()['last_updated']

    station_status_url = 'https://gbfs.velobixi.com/gbfs/2-2/en/station_status.json'
    status_response = requests.get(station_status_url)
    stations_status_list = status_response.json()['data']['stations']
    # status_update_time = status_response.json()['last_updated'] # == info_update_time

    stations_coords = map(get_coords, stations_info_list)
    stations_names = map(get_name, stations_info_list)
    stations_caps = map(get_capacity, stations_info_list)
    stations_num_bikes_av = map(get_num_bikes_available, stations_status_list) 
    stations_num_docks_av = map(get_num_docks_available, stations_status_list)
    stations_isFunctional = map(is_functional, stations_status_list)

    # turn this info into a dataframe (unnecessary ? overhead?) -> write to csv
    eval_coords = list(stations_coords)
    df = pl.DataFrame(
        {
            'name' : list(stations_names),
            # only write coords within montreal borders
            'lat' : list(map(lambda t: t[0] if t is not None else 0, eval_coords)), 
            'lon' : list(map(lambda t: t[1] if t is not None else 0, eval_coords)),
            'capacity' : list(stations_caps),
            'number available bikes' : list(stations_num_bikes_av),
            'number available docks' : list(stations_num_docks_av),
            'is functional' : list(stations_isFunctional)
        }
    )

    path = "./data/output.csv"
    write_header = not os.path.exists(path)
    with open(path, "a") as f:
        df.write_csv(f, include_header=write_header)
        
    # i wanna save the update time too, for the dashboard
    with open('./data/update_time.txt', 'a') as file_object:
        file_object.write(dt.datetime.fromtimestamp(info_update_time).strftime("%Y-%m-%d %H:%M:%S\n"))
        
    
# my cron will 
# 1. run this file (fetch.py) -> yields "output.csv" and "update_time.txt" 
# 2. run dashboard.py -> reads the above two files, and updates the dashboard (streamlit)
# 3. Right now data is in project folder. After Im done making stuff pretty, 
# I will move the data into a sql database and read it from there
if __name__ == "__main__":
    main()
