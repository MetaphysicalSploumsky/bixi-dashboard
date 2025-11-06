import streamlit as st 
import polars as pl
import pydeck as pdk
from millify import millify
import pandas as pd
import psycopg2
from env_var import ENDPOINT, PW, USERNAME, NAME, PORT

TOTAL_BIKES = 12600 # per bixi, approx.
MIN_LAT, MAX_LAT = 45.40, 45.55 # Montreal + Longueil
MIN_LON, MAX_LON = -73.70, -73.45

@st.cache_resource
def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=NAME, 
            user=USERNAME, 
            password=PW, 
            host=ENDPOINT, 
            port=PORT
        )
        return conn # cache 
    except (Exception, psycopg2.Error) as error:
        st.error(f"Error connecting to database: {error}")
        return None

@st.cache_data(ttl=300) # cache for 5 minutes
def load_latest_snapshot(_conn):
    query = """
        SELECT * FROM station_status_log
        WHERE fetched_at = (SELECT MAX(fetched_at) FROM station_status_log);
    """
    
    df = pl.read_database(query, _conn)
    
    if not df.is_empty():
        df = df.with_columns(
            pl.col("fetched_at").dt.convert_time_zone("America/Toronto")
        )
    
    df = df.rename({
        "is_functional": "is functional",
        "bikes_av": "number available bikes",
        "docks_av": "number available docks"
    })
    return df

@st.cache_data(ttl=300) 
def load_aggregate_history(_conn):

    query = "SELECT * FROM system_aggregate_log ORDER BY fetched_at ASC;"
    
    df_agg = pl.read_database(query, _conn)
    
    if not df_agg.is_empty():
        df_agg = df_agg.with_columns(
            pl.col("fetched_at").dt.convert_time_zone("America/Toronto")
        )
    
    df_agg = df_agg.rename({
        "fetched_at": "Time",
        "total_bikes_av": "Total Bikes Available",
        "total_docks_av": "Total Docks Available",
        "empty_stations": "Empty Stations",
        "full_stations": "Overflow Stations" 
    })
    return df_agg

@st.cache_data(ttl=300) 
def load_station_history(_conn):
    query = """
        SELECT name, bikes_av, fetched_at
        FROM station_status_log
        WHERE fetched_at IN (
            SELECT DISTINCT fetched_at
            FROM station_status_log
            ORDER BY fetched_at DESC
            LIMIT 18
        );
    """
    
    df_hist = pl.read_database(query, _conn)
    
    if not df_hist.is_empty():
        df_hist = df_hist.with_columns(
            pl.col("fetched_at").dt.convert_time_zone("America/Toronto")
        )
    
    df_hist = df_hist.rename({
        "fetched_at": "update_time",
        "bikes_av": "number available bikes"
    })
    return df_hist


st.write("""
# STM Strike — Can Bixi save us all?!
With public transit on strike all of November, Montrealers are turning to Bixi to get around.  
To handle the surge, the network has added extra drop-off stations and more staff to keep bikes balanced across the city.  

The metrics, map and line charts below will be updated every 5 minutes. Let's see how the system holds up! 🚲
""")

conn = get_db_connection()
if conn is None:
    st.stop()

df = load_latest_snapshot(conn)
df_agg = load_aggregate_history(conn)

if df.is_empty() or df_agg.is_empty():
    st.warning("No data found in the database")
    st.stop()



a, b, c = st.columns([1, 1.2, 1.2])

# 1. System-Wide Availability
bikes_available = df_agg.select(pl.last('Total Bikes Available')).item()
bikes_available_prev_update = df_agg.select(pl.col('Total Bikes Available')).tail(2).head(1).item()
delta_bikes_av = bikes_available - bikes_available_prev_update
a.metric("Total Bikes Available", f"{millify(bikes_available)} / {millify(TOTAL_BIKES)}", f'{delta_bikes_av}', border=True) 

# 2. "System Stress (Empty)"
num_stations_less_3bikes = df_agg.select(pl.last('Empty Stations')).item()
total_stations = df.select(pl.len()).item()
percentage_deserts = num_stations_less_3bikes / total_stations * 100
empty_prev = df_agg.select(pl.col('Empty Stations')).tail(2).head(1).item()
delta_empty = num_stations_less_3bikes - empty_prev
b.metric("Stations at < 3 bikes", f"{num_stations_less_3bikes} ({percentage_deserts:.2f} %)", f"{delta_empty}", border=True)

# 3. "System Stress (Full)"
num_stations_less_3docks = df_agg.select(pl.last('Overflow Stations')).item()
percentage_overflows = num_stations_less_3docks / total_stations * 100
full_prev = df_agg.select(pl.col('Overflow Stations')).tail(2).head(1).item()
delta_full = num_stations_less_3docks - full_prev
c.metric("Stations at < 3 docks", f"{num_stations_less_3docks} ({percentage_overflows:.2f} %)", f"{delta_full}", border=True)


# map
df_filtered = df.filter(
    (pl.col("is functional") == True) & 
    (pl.col("lat") >= MIN_LAT) &
    (pl.col("lat") <= MAX_LAT) &
    (pl.col("lon") >= MIN_LON) &
    (pl.col("lon") <= MAX_LON)
)

data = df_filtered.to_pandas()

def color_row(row):
    if row["number available bikes"] < 3:
        return [255, 75, 75]  # red
    elif row["number available docks"] < 3:
        return [255, 200, 0]  # yellow 
    else:
        return [0, 180, 0]  # green normal

data["color"] = data.apply(color_row, axis=1)

layer = pdk.Layer(
    "ScatterplotLayer",
    data=data,
    get_position=["lon", "lat"],
    get_fill_color="color",
    get_radius=30, 
    pickable=True,
    opacity=0.8,
)

view_state = pdk.ViewState(latitude=45.4996, longitude=-73.5668, zoom=13)

deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    map_style="light",  
    tooltip={
        'html': '{name} <br> Bikes: {number available bikes} <br> Open docks: {number available docks}',
        'style': {'color': 'white'}
    } # type: ignore
)


station_history_df = load_station_history(conn).to_pandas()
station_names = sorted(station_history_df["name"].unique())

col_map, col_chart = st.columns([2, 1])

with col_map:
    st.pydeck_chart(deck)

with col_chart:
    st.write("#### Bikes at Station (Last ~12 Hours)")
    selected_station = st.selectbox("Select a station", station_names)
    if selected_station:
        station_df = station_history_df[station_history_df["name"] == selected_station]
        station_df = station_df.sort_values("update_time") # type: ignore
        station_df["update_time"] = pd.to_datetime(station_df["update_time"])
        station_df["time_label"] = station_df["update_time"].dt.strftime("%H:%M")
        
        station_chart_df = station_df[["time_label", "number available bikes"]].set_index("time_label")
        
        st.line_chart(
            station_chart_df,
            width='stretch',
            height=250
        )

st.write("## Global Evolution")

df_agg_pd = df_agg.to_pandas()
df_agg_pd = df_agg_pd.set_index("Time")

st.write("### Station Stress")
st.line_chart(df_agg_pd[["Empty Stations", "Overflow Stations"]],
              width='stretch', color=['#FF0000', '#FFC800'],
              )

st.write("### Bikes and Docks")
st.line_chart(df_agg_pd[["Total Bikes Available", "Total Docks Available"]],
              width='stretch')