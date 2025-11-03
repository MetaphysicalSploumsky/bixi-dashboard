# read from the csv, updates the dashboard
# dashboard.py
import streamlit as st 
from streamlit_folium import st_folium
import polars as pl
import folium

st.write("""
# My first app
Hello *bixi!*
""")

df = pl.read_csv('./output.csv')

city_centre_coords = [45.4996, -73.5668]
m = folium.Map(location=city_centre_coords, zoom_start=14)

for row in df.iter_rows(named=True):
    if row['is functional'] == 1 and row['number available bikes'] >= 10:
        folium.Marker(
            location=[row['lat'], row['lon']],
            icon=folium.Icon(color="green"),
            popup=row['name']
        ).add_to(m)
    elif row['is functional'] == 1 and row['number available bikes'] < 5:
        folium.Marker(
            location=[row['lat'], row['lon']],
            icon=folium.Icon(color="red"),
            popup=row['name']
        ).add_to(m)

st_data = st_folium(m, width=725)

# streamlit run dashboard.py