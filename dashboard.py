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

# compute avg lon and lat, to generate initial map position 
clean_lon = df.select(pl.col('lon')).filter(pl.col('lon').is_not_null())
clean_lat = df.select(pl.col('lat')).filter(pl.col('lat').is_not_null())

avg_lon = clean_lon.sum() / clean_lon.count()
avg_lat = clean_lat.sum() / clean_lat.count()

avg_coord = [avg_lat[0].item(), avg_lon.item()]

# generate the map
m = folium.Map(location=avg_coord, zoom_start=16)

for lat, lon in zip(clean_lat.iter_rows(), clean_lon.iter_rows()):
    folium.Marker(
        [lat[0], lon[0]]
    ).add_to(m)

st_data = st_folium(m, width=725)

# streamlit run dashboard.py