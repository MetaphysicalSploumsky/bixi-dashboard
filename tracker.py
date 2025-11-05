# fetch the most recent timestamp from the station_status_log table
# compute aggregates (total bikes, total docks, empty/full stations) for that timestamp
# insert these aggregates into the system_aggregate_log table
from env_var import ENDPOINT, PW, USERNAME, NAME, PORT
import psycopg2

def main():
    conn = None
    try:
        conn = psycopg2.connect(dbname=NAME, user=USERNAME, password=PW, host=ENDPOINT)
    except (Exception, psycopg2.Error) as error:
        print(f"Error connecting to database: {error}")
        return 
        
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
        finally:
            if conn is not None:
                conn.close()
                print("Database connection closed.")

if __name__ == "__main__":
    main()