# conn to db and delete older entries
# once a week
import os
import psycopg2

def main():
    conn = None
    try:
        conn = psycopg2.connect(
                dbname=os.environ['DBNAME'], 
                user=os.environ['DBUSERNAME'], 
                password=os.environ['PW'], 
                host=os.environ['ENDPOINT']
            )
        
        query = """
        DELETE FROM station_status_log 
        WHERE fetched_at < NOW() - INTERVAL '1 day';
        """
        
        with conn:
            with conn.cursor() as curs:
                curs.execute(query)
                print("Deleted entries older than 1 day.")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()
        
    
def handler(event, context):
    try:
        main()
    except Exception as e:
        print(f"Unhandled: {e}")

if __name__ == "__main__":
    main()