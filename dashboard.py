# read from the csv, updates the dashboard
# dashboard.py
import streamlit as st 
from streamlit_folium import st_folium
import polars as pl
import folium
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


city_centre_coords = [45.4996, -73.5668]
m = folium.Map(location=city_centre_coords, zoom_start=14)
# If you want to dynamically add or remove items from the map,
# add them to a FeatureGroup and pass it to st_folium
fg = folium.FeatureGroup(name="Stations") 
for row in df.iter_rows(named=True):
    if row['is functional'] == 1:
        fg.add_child(
        folium.Marker(
            location=[row['lat'], row['lon']],
            popup=row['name'],
            icon=folium.Icon(color="red") if row['number available bikes'] <= 5 else folium.Icon(color="green")
        )
    )

out = st_folium(
    m,
    feature_group_to_add=fg,
    zoom=17,
    center=(45.4996, -73.5668),
    width=1200,
    height=500,
)

# streamlit run dashboard.py