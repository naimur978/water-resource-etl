#!/usr/bin/env python3

import numpy as np
import pandas as pd
import json
import aiohttp
import asyncio
import os
from pathlib import Path
import datetime as dt
from datetime import datetime, timedelta
from tqdm import tqdm
import matplotlib.pyplot as plt
import seaborn as sns

# Create output directory and its subdirectories
output_dir = Path("output")
output_dir.mkdir(exist_ok=True)
(output_dir / "metadata").mkdir(exist_ok=True)
(output_dir / "sensor_data").mkdir(exist_ok=True)

# Sensors Data Endpoints
URL_RESERVOIR_DATA = "https://aplicacions.aca.gencat.cat/sdim2/apirest/data/EMBASSAMENT-EST"     # updated every 5min
URL_GAUGE_DATA = "https://aplicacions.aca.gencat.cat/sdim2/apirest/data/AFORAMENT-EST"
URL_PLUVIOMETER_DATA = "https://aplicacions.aca.gencat.cat/sdim2/apirest/data/PLUVIOMETREACA-EST"
URL_PIEZOMETER_DATA = "https://aplicacions.aca.gencat.cat/sdim2/apirest/data/PIEZOMETRE-EST"

urls_data = [URL_RESERVOIR_DATA, URL_GAUGE_DATA, URL_PLUVIOMETER_DATA, URL_PIEZOMETER_DATA]

async def get_data_async(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"Request failed with status code {response.status} for {url}")
                return None
    except Exception as e:
        print(f"Error fetching data from {url}: {str(e)}")
        return None

async def fetch_time_range_data(session, url, dt_from, dt_to):
    query_url = url + f"/?limit=5&from={dt_from.strftime('%d/%m/%YT%H:%M:%S')}&to={dt_to.strftime('%d/%m/%YT%H:%M:%S')}"
    return await get_data_async(session, query_url)

async def get_sensors_data_day_async(read_date_dt=None, groups_cols_ids=None):
    if read_date_dt is None:
        read_date_dt = datetime.now() - timedelta(days=1)
    
    all_dfs_sensors_day = []
    
    async with aiohttp.ClientSession() as session:
        for cols_ids, url_data in zip(groups_cols_ids, urls_data):
            df_sensor_data_day = pd.DataFrame(columns=cols_ids)
            time_ranges = [read_date_dt.replace(hour=i, minute=0, second=0, microsecond=0) for i in [0, 6, 12, 18]]
            
            # Create tasks for all time ranges
            tasks = []
            for tr in time_ranges:
                dt_from = tr - timedelta(hours=6)
                dt_to = tr + timedelta(hours=6)
                tasks.append(fetch_time_range_data(session, url_data, dt_from, dt_to))
            
            # Execute all tasks concurrently
            results = await asyncio.gather(*tasks)
            
            # Process results
            for tr, data_day in zip(time_ranges, results):
                if data_day and "sensors" in data_day:
                    for sensor in data_day["sensors"]:
                        sensor_id = sensor["sensor"]
                        if sensor["observations"]:
                            value = np.mean([float(obs["value"]) for obs in sensor["observations"]]).round(4)
                            df_sensor_data_day.loc[tr, sensor_id] = value
            
            all_dfs_sensors_day.append(df_sensor_data_day)
    
    return all_dfs_sensors_day

def load_metadata():
    METADATA_PATH = Path("./dataset/metadata")
    metadata = {}
    
    metadata['reservoir'] = pd.read_csv(METADATA_PATH / "reservoir_sensors_metadata.csv")
    metadata['gauge'] = pd.read_csv(METADATA_PATH / "gauge_sensors_metadata.csv")
    metadata['pluviometer'] = pd.read_csv(METADATA_PATH / "pluviometer_sensors_metadata.csv")
    metadata['piezometer'] = pd.read_csv(METADATA_PATH / "piezometer_sensors_metadata.csv")
    
    # Save metadata files in the output directory
    metadata_dir = output_dir / "metadata"
    for key in metadata:
        metadata[key].to_csv(metadata_dir / f"{key}_sensors_metadata.csv", index=False)
        print(f"Saved metadata: {metadata_dir / f'{key}_sensors_metadata.csv'}")
    
    return metadata

async def main():
    # Load metadata
    metadata = load_metadata()
    
    # Prepare sensor IDs
    groups_cols_ids = [
        metadata['reservoir']["sensor_id"],
        metadata['gauge']["sensor_id"],
        metadata['pluviometer']["sensor_id"],
        metadata['piezometer']["sensor_id"]
    ]
    
    # Paths for sensor data
    DATA_PATH = Path("./dataset")
    list_paths_old_data = [
        DATA_PATH / f"{sensor}_sensors_reads.csv"
        for sensor in ['reservoir', 'gauge', 'pluviometer', 'piezometer']
    ]
    
    # Load existing data if available
    if os.path.exists(list_paths_old_data[0]):
        list_all_old_data = [
            pd.read_csv(path_old_data, parse_dates=[0], index_col=0)
            for path_old_data in list_paths_old_data
        ]
    else:
        list_all_old_data = None

    if list_all_old_data is None:
        # First run - fetch last 3 months of data
        list_all_data = [[] for _ in range(4)]
        
        with tqdm(range(89, 0, -1)) as pbar:
            for d in pbar:
                pbar.set_description(f"Fetching day -{d}")
                read_date_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=d)
                all_dfs_sensors_day = await get_sensors_data_day_async(read_date_dt=read_date_dt, groups_cols_ids=groups_cols_ids)
                
                for i in range(4):
                    list_all_data[i].append(all_dfs_sensors_day[i])
        
        list_all_old_data = [pd.concat(dfs_sensor_data, axis=0) for dfs_sensor_data in list_all_data]
        
    else:
        # Daily update
        print("Updating previous day's sensor reads...")
        read_date_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        last_dfs_sensors_day = await get_sensors_data_day_async(read_date_dt=read_date_dt, groups_cols_ids=groups_cols_ids)
        
        # Combine new data with old data
        list_all_old_data = [
            pd.concat([old_df_sensor, last_df_sensor], axis=0)
            for old_df_sensor, last_df_sensor in zip(list_all_old_data, last_dfs_sensors_day)
        ]
        
        # Sort and remove duplicates
        list_all_old_data = [
            df_sensor.sort_index(ascending=False) for df_sensor in list_all_old_data
        ]
        list_all_old_data = [
            df_sensor[~df_sensor.index.duplicated(keep="first")] 
            for df_sensor in list_all_old_data
        ]
    
    # Save updated data to output directory
    sensor_data_dir = output_dir / "sensor_data"
    for sensor_name, df_sensor_data in zip(
        ["reservoir", "gauge", "pluviometer", "piezometer"],
        list_all_old_data
    ):
        output_file = sensor_data_dir / f"{sensor_name}_sensors_reads.csv"
        df_sensor_data.to_csv(output_file)
        print(f"Saved sensor data: {output_file}")
    
    # Print statistics
    print("\nStatistics:")
    print("Rows:", len(list_all_old_data[0]))
    print("Total Sensors:", sum([len(s.columns) for s in list_all_old_data]))
    print("From:", list_all_old_data[0].index[0])
    print("To:  ", list_all_old_data[0].index[-1])

if __name__ == "__main__":
    asyncio.run(main())
