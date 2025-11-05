#!/usr/bin/env python3
import requests
import datetime as dt
import polars as pl
from typing import Tuple

from env_var import ENDPOINT, PW, USERNAME, NAME, PORT
import psycopg2
import psycopg2.extras

# fetches data, transform, update the db
    
def main(): 
    station_info_url = 'https://gbfs.velobixi.com/gbfs/2-2/en/station_information.json'
    info_response = requests.get(station_info_url)
    stations_info_list = info_response.json()['data']['stations']
    info_update_time = info_response.json()['last_updated']

    station_status_url = 'https://gbfs.velobixi.com/gbfs/2-2/en/station_status.json'
    status_response = requests.get(station_status_url)
    stations_status_list = status_response.json()['data']['stations']

    try:
        df_info = pl.DataFrame(stations_info_list)
        df_status = pl.DataFrame(stations_status_list)

        df_merged = df_info.join(df_status, on="station_id", how="inner")

        df = df_merged.select(
            pl.col("name"),
            pl.col("lat").fill_null(0.0), 
            pl.col("lon").fill_null(0.0), 
            pl.col("capacity"),
            pl.col("num_bikes_available").alias("bikes_av"),
            pl.col("num_docks_available").alias("docks_av"),
            (
                (pl.col("is_installed") == 1) & 
                (pl.col("is_renting") == 1) & 
                (pl.col("is_returning") == 1)
            )
            .alias("is_functional")
            .fill_null(False) 
        )
    except Exception as e:
        print(f"Error processing data: {e}")
        return 

    try:
        fetch_timestamp = dt.datetime.fromtimestamp(info_update_time)
    except NameError:
        print("'info_update_time' not av. Using current time ")
        fetch_timestamp = dt.datetime.now() 

    conn = None
    try:
        conn = psycopg2.connect(dbname=NAME, user=USERNAME, password=PW, host=ENDPOINT)
    except (Exception, psycopg2.Error) as error:
        print(f"Error connecting to database: {error}")
        return 

    if conn:
        command = """
            INSERT INTO station_status_log
            (name, lat, lon, capacity, bikes_av, docks_av, is_functional, fetched_at) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s); 
            """
        
        
        data_to_insert = [row + (fetch_timestamp,) for row in df.rows()]
        
        try:
            with conn:
                with conn.cursor() as curs:
                    psycopg2.extras.execute_batch(curs, command, data_to_insert)
                    print(f"Successfully batch-inserted {len(data_to_insert)} rows.")
                        
        except (Exception, psycopg2.Error) as error:
            print(f"Error during batch insert: {error}")
        
        finally:
            if conn is not None:
                conn.close()
                print("Database connection closed.")
            
        
if __name__ == "__main__":
    main()
