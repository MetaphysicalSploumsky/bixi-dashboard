import streamlit as st 
import polars as pl
import pydeck as pdk
from millify import millify
from collections import deque
from io import StringIO
import pandas as pd

TOTAL_BIKES = 12600 # per bixi, approx.
MIN_LAT, MAX_LAT = 45.40, 45.55 # Montreal + Longueil
MIN_LON, MAX_LON = -73.70, -73.45


st.write("""
# STM Strike — Bixi Network Under Pressure  
With public transit on strike all of November, Montreal is turning to Bixi to get around.  
To handle the surge, the network has added extra drop-off stations and more staff to keep bikes balanced across the city.  

Let's see how the system holds up! 🚲
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


df_filtered = df.filter(
    (pl.col("is functional") == 1) &
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

# reads last 18 updates (6 hours) for all stations
def load_station_history():
    num_stations = 1031
    num_updates = 18
    num_lines = num_stations * num_updates

    csv_path = "./data/output.csv"
    txt_path = "./data/update_time.txt"

    # Read recent data from CSV
    with open(csv_path, "r") as f:
        header = f.readline().strip()
        last_lines = deque(f, maxlen=num_lines)
    csv_data = "\n".join([header] + [line.strip() for line in last_lines])
    df = pl.read_csv(StringIO(csv_data), has_header=True)

    # Read timestamps
    with open(txt_path, "r") as f:
        time_lines = [line.strip() for line in deque(f, maxlen=num_updates)]

    df = df.with_columns(
        pl.Series("update_time", time_lines * num_stations)
    )

    return df

station_history_df = load_station_history().to_pandas()
station_names = sorted(station_history_df["name"].unique())

col_map, col_chart = st.columns([2, 1])

with col_map:
    st.pydeck_chart(deck)

with col_chart:
    st.write("#### Bikes at Station (Last 12 Hours)")
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

df_agg = df_agg.with_columns(pl.col("Time").str.strip_chars().str.to_datetime("%Y-%m-%d %H:%M:%S")).to_pandas()
df_agg = df_agg.set_index("Time")

st.write("### Station Stress")
st.line_chart(df_agg[["Empty Stations", "Overflow Stations"]],
              width='stretch', color=['#FF0000', '#FFC800'],
              )

st.write("### Bikes and Docks")
st.line_chart(df_agg[["Total Bikes Available", "Total Docks Available"]],
              width='stretch')

