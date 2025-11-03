# read from the csv, updates the dashboard
# dashboard.py
import streamlit as st 
from streamlit_folium import st_folium
import polars as pl
import pydeck as pdk
from millify import millify

st.write("""
# STM Strike - Bixi Network Stress
Long live bixi!
""")

df = pl.read_csv('./output.csv')

a, b, c = st.columns([1, 1.2, 1])
# 1. System-Wide Availability; gauge or metric: "Total Bikes: 4,520 / 10,000"
bikes_available = (
    df.select(pl.col('number available bikes'))
    .sum()
    .item()
)
total_bikes = 12600 # per bixi, approx.
a.metric("Total Bikes Available", f"{millify(bikes_available)} / {millify(total_bikes)}", '+ 9', border=True) # delta +9 is placeholder. Should be latest fetch - fetch before. Maybe save changes in "deltas.txt" or maybe save aggregate counts in "counts.txt"
# so computations post fetches are easy
# 2. "System Stress (Empty)" (A metric: "Stations at < 3 bikes: 120") 
num_stations_less_3bikes = (
    df.filter(pl.col('number available bikes') < 3)
      .select(pl.count())
      .item()
)
total_stations = df.select(pl.count()).item()
percentage_deserts = num_stations_less_3bikes / total_stations * 100
b.metric("Stations at < 3 bikes", f"{num_stations_less_3bikes} ({percentage_deserts:.2f} %)", "-5%", border=True)
# 3. "System Stress (Full)" (A metric: "Stations at < 3 docks: 85")
num_stations_less_3docks = (
    df.filter(pl.col('number available docks') < 3)
    .select(pl.count())
    .item()
)
percentage_overflows = num_stations_less_3docks / total_stations * 100
c.metric("Stations at < 3 docks", f"{num_stations_less_3docks} ({percentage_overflows:.2f} %)", "5%", border=True)
 

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

    