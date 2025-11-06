import os
import requests
import datetime as dt
import polars as pl
import psycopg2
import psycopg2.extras
import pytz
def main():

    utc = pytz.utc
    montreal_tz = pytz.timezone("America/Toronto")
    
    def fetch(): 
        bool = None
        
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
            return False

        try:
            montreal_tz = pytz.timezone("America/Toronto")
            utc_timestamp = dt.datetime.fromtimestamp(info_update_time, dt.timezone.utc)
            fetch_timestamp = utc_timestamp.astimezone(montreal_tz)
            print(f"Timestamp converted to Montreal time: {fetch_timestamp}")
        
        except Exception as e:
            print(f"Error processing 'info_update_time': {e}. Using current time.")
            montreal_tz = pytz.timezone("America/Toronto")
            utc_now = dt.datetime.now(dt.timezone.utc)
            fetch_timestamp = utc_now.astimezone(montreal_tz)

        conn = None
        try:
            conn = psycopg2.connect(
                dbname=os.environ['NAME'], 
                user=os.environ['DBUSERNAME'], 
                password=os.environ['PW'], 
                host=os.environ['ENDPOINT']
            )
        except (Exception, psycopg2.Error) as error:
            print(f"Error connecting to database: {error}")
            return False

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
                        print(f"Successfully inserted {len(data_to_insert)} rows.")
                        bool = True
                            
            except (Exception, psycopg2.Error) as error:
                print(f"Error during insert: {error}")
                bool = False
            
            finally:
                if conn is not None:
                    conn.close()
                    print("Database connection closed.")
                    return bool
    
    def aggregate():
        conn = None
            
        try:
            conn = psycopg2.connect(
            dbname=os.environ['NAME'], 
            user=os.environ['DBUSERNAME'], 
            password=os.environ['PW'], 
            host=os.environ['ENDPOINT']
        )
        except (Exception, psycopg2.Error) as error:
            print(f"Error connecting to database: {error}")
            return False
                
        if conn:
            try:
                with conn:
                    with conn.cursor() as curs:
                    
                        curs.execute("SELECT MAX(fetched_at) FROM station_status_log;")
                        row = curs.fetchone()
                    
                        if not (row and row[0]):
                            print("No data found in station_status_log.")
                            return 
                            
                        most_recent = row[0]
                        print(f"Processing data for timestamp: {most_recent}")

                        aggregate_command = """
                            SELECT
                                SUM(bikes_av),
                                SUM(docks_av),
                                COUNT(*) FILTER (WHERE bikes_av < 3),
                                COUNT(*) FILTER (WHERE docks_av < 3)
                            FROM station_status_log
                            WHERE fetched_at = %s;
                        """
                        curs.execute(aggregate_command, (most_recent,))
                        agg_row = curs.fetchone()
                
                        if not agg_row:
                            print(f"Could not fetch aggregates for {most_recent}.")
                            raise Exception("Aggregate query returned no rows")

                        insert_command = """
                            INSERT INTO system_aggregate_log
                            (total_bikes_av, total_docks_av, empty_stations, full_stations, fetched_at)
                            VALUES (%s, %s, %s, %s, %s)
                        """
                        vals = agg_row + (most_recent,)
                        
                        curs.execute(insert_command, vals)
                        print("Successfully inserted aggregate data.")
                                
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"Database error occurred: {error}. Transaction will be rolled back.")
                return False
            finally:
                if conn is not None:
                    conn.close()
                    print("Database connection closed.")
                    return True
                        
    if fetch():
        print("Fetch successfull. Aggregating...")
        aggregate()
        print("Lambda successful.")
        return True
    else:
        print("Lambda unsuccessful.")
        return False
            
def handler(event, context):
    try:
        return main()
    except Exception as e:
        print(f"Unhandled: {e}")
        return False
    
# code works. 
# after lunch : run the container -> export needed variables -> see print statements locally
# push to ECR -> create lambda function -> test on cloud -> see print statements and changes in db on aws
# done! 
# when pushing : bixi-lambda

if __name__ == "__main__":
    main()
    
# docker buildx build --platform linux/arm64 --provenance=false -t bixi-lambda:latest .
# docker run --platform linux/arm64 -p 9000:8080 bixi-lambda:latest