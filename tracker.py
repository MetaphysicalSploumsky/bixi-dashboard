# read 1031 most recent lines from output.csv
# computes aggregate data for timeseries plots
# saves to database 
# header = [Total Bikes Available, Total Docks Available, Total Empty, Total Full, Datetime]
# datetime from fetch.py, currently in update_time.txt
from collections import deque
import polars as pl
import os

n = 1031
csv_path = "./data/output.csv"
time_path = "./data/update_time.txt"

with open(csv_path, "r") as f:
    header = f.readline().strip()
    last_lines = deque(f, maxlen=n)

csv_data = "\n".join([header] + [line.strip() for line in last_lines])
df = pl.read_csv(csv_data.encode(), has_header=True)

with open(time_path, "r") as f:
    time = deque(f, maxlen=1)[0].strip() # last line; example: 2025-11-03 10:39:47

tot_bikes_av = (
    df.select(pl.col('number available bikes'))
    .sum()
    .item()
)

tot_docks_av = (
    df.select(pl.col('number available docks'))
    .sum()
    .item()
)

num_stations_less_3bikes = (
    df.filter(pl.col('number available bikes') < 3)
      .select(pl.len())
      .item()
)

num_stations_less_3docks = (
    df.filter(pl.col('number available docks') < 3)
    .select(pl.len())
    .item()
)

to_write = pl.DataFrame(
    {
        "Total Bikes Available" : tot_bikes_av,
        "Total Docks Available" : tot_docks_av, 
        "Empty Stations" : num_stations_less_3bikes,
        "Overflow Stations" : num_stations_less_3docks,
        "Time" : time
    }
)

path = "./data/aggregate.csv"
write_header = not os.path.exists(path)

with open(path, "a") as f:
    to_write.write_csv(f, include_header=write_header)

