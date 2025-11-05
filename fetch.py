#!/usr/bin/env python3
import requests
import datetime as dt
import polars as pl
from typing import Tuple
import os

from env_var import ENDPOINT, PW, USERNAME, NAME, PORT
import psycopg2

# fetches data, transform, update the db 
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
    
    try:
        fetch_timestamp = dt.datetime.fromtimestamp(info_update_time)
    except NameError:
        print("'info_update_time' not set. Using current time ")
        fetch_timestamp = dt.datetime.now() 

    conn = None
    try:
        conn = psycopg2.connect(dbname=NAME, user=USERNAME, password=PW, host=ENDPOINT)
    except (Exception, psycopg2.Error) as error:
        print(f"Error connecting to database: {error}")

    if conn:
        id_counter = 0 

        command = """
            INSERT INTO station_status_log
            (station_id, name, lat, lon, capacity, bikes_av, docks_av, is_functional, fetched_at) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s); 
            """
        
        try:
            with conn:
                with conn.cursor() as curs:
                    for row in df.iter_rows():
                    
                        id_counter += 1
                        
                        data_tuple = (
                            str(id_counter),       # %s (id)
                            row[0],           # %s (name)
                            row[1],           # %s (lat)
                            row[2],           # %s (lon)
                            row[3],           # %s (capacity)
                            row[4],           # %s (bikes_av)
                            row[5],           # %s (docks_av)
                            (row[6] == 1),           # %s (is_func, as bool )
                            (fetch_timestamp)   # %s (timestamp of fetch)
                        )
                        
                        curs.execute(command, data_tuple)
        
            print(f"Successfully inserted {id_counter} rows.")

        except (Exception, psycopg2.Error) as error:
            print(f"Error inserting record: {error}")
        
        finally:
            if conn is not None:
                conn.close()
                print("Database connection closed.")
            
        
if __name__ == "__main__":
    main()
