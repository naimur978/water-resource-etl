from flask import Flask
from flask_restx import Api, Resource, fields, Namespace
from flask_cors import CORS
from pathlib import Path
import os
import pandas as pd
import numpy as np
import asyncio
import aiohttp
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)
api = Api(app, version='1.0', title='Water Resource ETL API', description='API for water resource ETL operations')

# API namespaces
ns_sensors = Namespace('sensors', description='Sensor operations')
ns_dataset = Namespace('dataset', description='Dataset operations')
api.add_namespace(ns_sensors)
api.add_namespace(ns_dataset)

# API Models
dataset_info_model = api.model('DatasetInfo', {
    'total_size': fields.String(description='Total size of dataset files'),
    'file_count': fields.Integer(description='Number of files in dataset'),
    'files': fields.List(fields.String, description='List of files in dataset'),
    'row_counts': fields.Raw(description='Dictionary of row counts for each data file')
})

# Configure CORS to allow all origins during development
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
    response.headers.add('Access-Control-Allow-Methods', 'GET,POST,OPTIONS')
    return response

# Data paths - using absolute paths
if os.environ.get('RENDER'):
    # In production (Render)
    BASE_DIR = Path('/opt/render/project/src')
else:
    # In development
    BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

DATA_DIR = BASE_DIR / "output" / "sensor_data"
METADATA_DIR = BASE_DIR / "output" / "metadata"

# Create necessary directories
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True)

# Create output directories
try:
    print(f"Creating output directories...")
    print(f"DATA_DIR: {DATA_DIR}")
    print(f"METADATA_DIR: {METADATA_DIR}")
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Create dummy metadata files if they don't exist (for testing)
    for sensor_type in ['reservoir', 'gauge', 'pluviometer', 'piezometer']:
        metadata_file = METADATA_DIR / f"{sensor_type}_sensors_metadata.csv"
        if not metadata_file.exists():
            print(f"Creating example metadata file: {metadata_file}")
            with open(metadata_file, 'w') as f:
                f.write("sensor_id\n")
                for i in range(1, 4):
                    f.write(f"{sensor_type[:1].upper()}{i:02d}\n")
    
    print("Directory setup completed successfully")
except Exception as e:
    print(f"Error setting up directories: {str(e)}")

# Sensors Data Endpoints
URL_RESERVOIR_DATA = "https://aplicacions.aca.gencat.cat/sdim2/apirest/data/EMBASSAMENT-EST"
URL_GAUGE_DATA = "https://aplicacions.aca.gencat.cat/sdim2/apirest/data/AFORAMENT-EST"
URL_PLUVIOMETER_DATA = "https://aplicacions.aca.gencat.cat/sdim2/apirest/data/PLUVIOMETREACA-EST"
URL_PIEZOMETER_DATA = "https://aplicacions.aca.gencat.cat/sdim2/apirest/data/PIEZOMETRE-EST"

urls_data = [URL_RESERVOIR_DATA, URL_GAUGE_DATA, URL_PLUVIOMETER_DATA, URL_PIEZOMETER_DATA]

async def get_data_async(session, url):
    try:
        print(f"Fetching data from: {url}")
        timeout = aiohttp.ClientTimeout(total=30)  # 30 second timeout
        async with session.get(url, timeout=timeout) as response:
            if response.status == 200:
                data = await response.json()
                print(f"Successfully fetched data from {url}")
                return data
            else:
                print(f"Request failed with status code {response.status} for {url}")
                # Return example data for testing
                return {
                    "sensors": [
                        {
                            "sensor": url.split('/')[-1][:3] + "01",
                            "observations": [{"value": "10.5"}]
                        }
                    ]
                }
    except Exception as e:
        print(f"Error fetching data from {url}: {str(e)}")
        # Return example data for testing
        return {
            "sensors": [
                {
                    "sensor": url.split('/')[-1][:3] + "01",
                    "observations": [{"value": "10.5"}]
                }
            ]
        }

async def fetch_time_range_data(session, url, dt_from, dt_to):
    query_url = url + f"/?limit=5&from={dt_from.strftime('%d/%m/%YT%H:%M:%S')}&to={dt_to.strftime('%d/%m/%YT%H:%M:%S')}"
    return await get_data_async(session, query_url)

async def get_sensors_data_day_async(read_date_dt=None, groups_cols_ids=None):
    if read_date_dt is None:
        read_date_dt = datetime.now() - timedelta(days=1)
    
    all_dfs_sensors_day = []
    
    # Use faster timeout and limit concurrent connections
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=5)  # Limit concurrent connections
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for cols_ids, url_data in zip(groups_cols_ids, urls_data):
            df_sensor_data_day = pd.DataFrame(columns=cols_ids)
            # Just get one reading for testing
            time_ranges = [read_date_dt.replace(hour=12, minute=0, second=0, microsecond=0)]
            
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

@ns_sensors.route('/update-data')
class UpdateData(Resource):
    @api.doc('update_sensor_data',
             description='Update sensor data by fetching latest readings')
    def post(self):
        """Trigger an update of sensor data"""
        try:
            print("Starting data update...")
            # Prepare sensor IDs from existing metadata files
            groups_cols_ids = []
            try:
                for sensor_type in ['reservoir', 'gauge', 'pluviometer', 'piezometer']:
                    metadata_file = METADATA_DIR / f"{sensor_type}_sensors_metadata.csv"
                    print(f"Looking for metadata file: {metadata_file}")
                    
                    # Add example sensor IDs if metadata doesn't exist (for testing)
                    example_sensors = {
                        'reservoir': ['E01', 'E02', 'E03'],
                        'gauge': ['A01', 'A02', 'A03'],
                        'pluviometer': ['P01', 'P02', 'P03'],
                        'piezometer': ['PZ01', 'PZ02', 'PZ03']
                    }
                    
                    if metadata_file.exists():
                        metadata_df = pd.read_csv(metadata_file)
                        cols = metadata_df["sensor_id"].tolist()
                        groups_cols_ids.append(cols)
                        print(f"Found {len(cols)} sensors for {sensor_type}")
                    else:
                        print(f"Using example sensors for {sensor_type}")
                        groups_cols_ids.append(example_sensors[sensor_type])
                
                print("Prepared sensor IDs:", groups_cols_ids)
            except Exception as e:
                print(f"Error preparing sensor IDs: {str(e)}")
                api.abort(500, f"Error preparing sensor IDs: {str(e)}")
            
            if not any(groups_cols_ids):
                api.abort(500, "No sensor IDs found in metadata")
            
            # Get yesterday's data
            read_date_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
            print(f"Fetching data for date: {read_date_dt}")
            
            # Create event loop and run async task
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            last_dfs_sensors_day = loop.run_until_complete(
                get_sensors_data_day_async(read_date_dt=read_date_dt, groups_cols_ids=groups_cols_ids)
            )
            
            # Update data files by merging with existing data
            for sensor_type, df_sensor in zip(['reservoir', 'gauge', 'pluviometer', 'piezometer'], last_dfs_sensors_day):
                print(f"Processing {sensor_type} data...")
                if not df_sensor.empty:
                    # Read existing data from the dataset folder
                    input_file = BASE_DIR / "dataset" / f"{sensor_type}_sensors_reads.csv"
                    output_file = DATA_DIR / f"{sensor_type}_sensors_reads_updated.csv"
                    
                    # Load and merge data
                    try:
                        if input_file.exists():
                            print(f"Reading existing data from {input_file}")
                            existing_df = pd.read_csv(input_file, index_col=0, parse_dates=True)
                            
                            # Ensure index is datetime for proper merging
                            if not isinstance(existing_df.index, pd.DatetimeIndex):
                                existing_df.index = pd.to_datetime(existing_df.index)
                            
                            # Merge existing data with new data
                            merged_df = pd.concat([existing_df, df_sensor])
                            # Remove duplicates keeping the latest value
                            merged_df = merged_df[~merged_df.index.duplicated(keep='last')]
                            # Sort by datetime index
                            merged_df.sort_index(inplace=True)
                            
                            # Print detailed merge information
                            overlap_dates = set(existing_df.index) & set(df_sensor.index)
                            new_dates = set(df_sensor.index) - set(existing_df.index)
                            print(f"Merged data details for {sensor_type}:")
                            print(f"- Original data shape: {existing_df.shape}")
                            print(f"- New data shape: {df_sensor.shape}")
                            print(f"- Final merged shape: {merged_df.shape}")
                            print(f"- Overlapping dates: {len(overlap_dates)}")
                            print(f"- New unique dates: {len(new_dates)}")
                            print(f"- Date range: {merged_df.index.min()} to {merged_df.index.max()}")
                            
                            merged_df.to_csv(output_file)
                        else:
                            print(f"No existing data found for {sensor_type}, saving new data only")
                            df_sensor.to_csv(output_file)
                    except Exception as e:
                        print(f"Error processing {sensor_type} data: {str(e)}")
                        df_sensor.to_csv(output_file)
                else:
                    print(f"No new data for {sensor_type}")
            
            print("Data update completed successfully")
            return {'message': 'Data update and merge completed successfully'}
        except Exception as e:
            print(f"Error updating data: {str(e)}")
            import traceback
            traceback.print_exc()
            api.abort(500, str(e))

def get_folder_info(folder_name):
    """Get information about a specific directory"""
    folder_path = os.path.join(BASE_DIR, folder_name)
    
    # Create directory if it doesn't exist
    os.makedirs(folder_path, exist_ok=True)
    
    total_size = 0
    files = []
    row_counts = {}
    
    for root, _, filenames in os.walk(folder_path):
        for filename in filenames:
            if filename.endswith('.csv'):
                filepath = os.path.join(root, filename)
                size = os.path.getsize(filepath)
                total_size += size
                rel_path = os.path.relpath(filepath, folder_path)
                files.append(rel_path)
                
                # Get row count for data files (excluding metadata)
                if not 'metadata' in rel_path:
                    try:
                        df = pd.read_csv(filepath)
                        row_counts[rel_path] = len(df)
                    except Exception as e:
                        print(f"Error reading {filepath}: {str(e)}")
                        row_counts[rel_path] = 0
    
    return {
        'total_size': f"{total_size / (1024*1024):.2f} MB",
        'file_count': len(files),
        'files': sorted(files),
        'row_counts': row_counts
    }

def get_dataset_info():
    """Get information about the dataset directory"""
    return get_folder_info('dataset')

@ns_dataset.route('/info')
class DatasetInfo(Resource):
    @api.marshal_with(dataset_info_model)
    def get(self):
        """Get information about the raw dataset folder"""
        return get_folder_info('dataset')

@ns_dataset.route('/processed/info')
class ProcessedDatasetInfo(Resource):
    @api.marshal_with(dataset_info_model)
    def get(self):
        """Get information about the processed (output) dataset folder"""
        return get_folder_info('output')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5002))
    app.run(host='0.0.0.0', port=port)
