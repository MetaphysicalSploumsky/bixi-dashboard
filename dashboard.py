# read from the csv, updates the dashboard
# dashboard.py
import streamlit as st 
import polars as pl
import pydeck as pdk
from millify import millify
from collections import deque
from numpy.random import default_rng as rng
import pandas as pd

TOTAL_BIKES = 12600 # per bixi, approx.


st.write("""
# STM Strike - Bixi Network Stress
Long live bixi!
""")



# read 1031 most recent entries only (bottom up + header)
n = 1031
csv_path = "./data/output.csv"

with open(csv_path, "r") as f:
    header = f.readline().strip()
    last_lines = deque(f, maxlen=n)

csv_data = "\n".join([header] + [line.strip() for line in last_lines])
df = pl.read_csv(csv_data.encode(), has_header=True) 

df_agg = pl.read_csv("./data/aggregate.csv", has_header=True) 
a, b, c = st.columns([1, 1.2, 1.2])

# 1. System-Wide Availability; "Total Bikes: 4,520 / 10,000"
bikes_available = df_agg.select(pl.last('Total Bikes Available')).item()
bikes_available_prev_update = df_agg.select(pl.col('Total Bikes Available')).tail(2).head(1).item()
delta_bikes_av = bikes_available - bikes_available_prev_update
a.metric("Total Bikes Available", f"{millify(bikes_available)} / {millify(TOTAL_BIKES)}", f'{delta_bikes_av}', border=True) 


# 2. "System Stress (Empty)" (A metric: "Stations at < 3 bikes: 120") 
num_stations_less_3bikes = df_agg.select(pl.last('Empty Stations')).item()
total_stations = df.select(pl.len()).item()
percentage_deserts = num_stations_less_3bikes / total_stations * 100
empty_prev = df_agg.select(pl.col('Empty Stations')).tail(2).head(1).item()
delta_empty = num_stations_less_3bikes - empty_prev
b.metric("Stations at < 3 bikes", f"{num_stations_less_3bikes} ({percentage_deserts:.2f} %)", f"{delta_empty}", border=True)

# 3. "System Stress (Full)" (A metric: "Stations at < 3 docks: 85")
num_stations_less_3docks = df_agg.select(pl.last('Overflow Stations')).item()
percentage_overflows = num_stations_less_3docks / total_stations * 100
full_prev = df_agg.select(pl.col('Overflow Stations')).tail(2).head(1).item()
delta_full = num_stations_less_3docks - full_prev
c.metric("Stations at < 3 docks", f"{num_stations_less_3docks} ({percentage_overflows:.2f} %)", f"{delta_full}", border=True)
 

st.sidebar.header("Filters")
hide_empty = st.sidebar.checkbox("Hide almost empty stations")
hide_full = st.sidebar.checkbox("Hide almost full stations")

df_filtered = df.filter(pl.col("is functional") == 1)

if hide_empty:
    df_filtered = df_filtered.filter(pl.col("number available bikes") >= 3)
if hide_full:
    df_filtered = df_filtered.filter(pl.col("number available docks") >= 3)

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
    get_radius=30, # extra: decrease on zoom, increase on dezoom
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
        'style': {
            'color': 'white'
        }
    } # type: ignore
)
st.pydeck_chart(deck)


# aggregate time series
st.write("## Evolution")

df_agg = df_agg.with_columns(pl.col("Time").str.strip_chars().str.to_datetime("%Y-%m-%d %H:%M:%S")).to_pandas()
df_agg = df_agg.set_index("Time")

# currently from start to end
# later add options to limit to : last 24h, last 12h 
st.write("### Station Stress")
st.line_chart(df_agg[["Empty Stations", "Overflow Stations"]],
              width='stretch', color=['#FF0000', '#FFC800'])

st.write("### Bikes and Docks")
st.line_chart(df_agg[["Total Bikes Available", "Total Docks Available"]],
              width='stretch')

# the cron job is : 1. python3 fetch.py (to get new data into output.csv and update time into update_time.txt)
#                   2. python3 tracker.py (to compute aggregate data and get them into aggregate.csv)
# we would like to do this every 5 minutes.


    