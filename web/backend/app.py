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

# Store previous dataset info
previous_dataset_info = None

# API Models
dataset_info_model = api.model('DatasetInfo', {
    'total_size': fields.String(description='Total size of dataset files'),
    'file_count': fields.Integer(description='Number of files in dataset'),
    'files': fields.List(fields.String, description='List of files in dataset')
})

dataset_changes_model = api.model('DatasetChanges', {
    'added_files': fields.List(fields.String, description='Files that were added'),
    'modified_files': fields.List(fields.String, description='Files that were modified'),
    'size_change': fields.String(description='Change in total dataset size'),
    'previous_info': fields.Nested(dataset_info_model),
    'current_info': fields.Nested(dataset_info_model)
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
BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
DATA_DIR = BASE_DIR / "output" / "sensor_data"
METADATA_DIR = BASE_DIR / "output" / "metadata"

# Create output directories
DATA_DIR.mkdir(parents=True, exist_ok=True)
METADATA_DIR.mkdir(parents=True, exist_ok=True)

# Sensors Data Endpoints
URL_RESERVOIR_DATA = "https://aplicacions.aca.gencat.cat/sdim2/apirest/data/EMBASSAMENT-EST"
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
            for sensor_type in ['reservoir', 'gauge', 'pluviometer', 'piezometer']:
                metadata_file = METADATA_DIR / f"{sensor_type}_sensors_metadata.csv"
                if metadata_file.exists():
                    metadata_df = pd.read_csv(metadata_file)
                    cols = metadata_df["sensor_id"].tolist()
                    groups_cols_ids.append(cols)
                    print(f"Found {len(cols)} sensors for {sensor_type}")
                else:
                    print(f"Warning: No metadata available for {sensor_type}")
                    groups_cols_ids.append([])
            
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
            
            # Update data files
            for sensor_type, df_sensor in zip(['reservoir', 'gauge', 'pluviometer', 'piezometer'], last_dfs_sensors_day):
                print(f"Processing {sensor_type} data...")
                if not df_sensor.empty:
                    # Save to file
                    output_file = DATA_DIR / f"{sensor_type}_sensors_reads.csv"
                    print(f"Saving {sensor_type} data to {output_file}")
                    df_sensor.to_csv(output_file)
                    print(f"Saved data with shape: {df_sensor.shape}")
                else:
                    print(f"No new data for {sensor_type}")
            
            # Update dataset info for tracking changes
            update_dataset_info()
            
            print("Data update completed successfully")
            return {'message': 'Data updated successfully'}
        except Exception as e:
            print(f"Error updating data: {str(e)}")
            import traceback
            traceback.print_exc()
            api.abort(500, str(e))

def get_dataset_info():
    """Get information about the dataset directory"""
    output_dir = os.path.join(BASE_DIR, 'output')
    total_size = 0
    files = []
    
    for root, _, filenames in os.walk(output_dir):
        for filename in filenames:
            if filename.endswith('.csv'):
                filepath = os.path.join(root, filename)
                size = os.path.getsize(filepath)
                total_size += size
                files.append(os.path.relpath(filepath, output_dir))
    
    return {
        'total_size': f"{total_size / (1024*1024):.2f} MB",
        'file_count': len(files),
        'files': sorted(files)
    }

def calculate_dataset_changes(previous_info, current_info):
    """Calculate changes between two dataset states"""
    if not previous_info:
        return {
            'added_files': current_info['files'],
            'modified_files': [],
            'size_change': f"+{current_info['total_size']}",
            'previous_info': None,
            'current_info': current_info
        }
    
    previous_size = float(previous_info['total_size'].split()[0])
    current_size = float(current_info['total_size'].split()[0])
    size_change = current_size - previous_size
    
    previous_files = set(previous_info['files'])
    current_files = set(current_info['files'])
    
    added_files = list(current_files - previous_files)
    modified_files = [f for f in current_files.intersection(previous_files)
                     if os.path.getmtime(os.path.join(BASE_DIR, 'output', f)) > 
                     os.path.getmtime(os.path.join(BASE_DIR, 'output', '.last_update'))]
    
    return {
        'added_files': sorted(added_files),
        'modified_files': sorted(modified_files),
        'size_change': f"{'+' if size_change >= 0 else ''}{size_change:.2f} MB",
        'previous_info': previous_info,
        'current_info': current_info
    }

@ns_dataset.route('/info')
class DatasetInfo(Resource):
    @api.marshal_with(dataset_info_model)
    def get(self):
        """Get current dataset information"""
        return get_dataset_info()

@ns_dataset.route('/changes')
class DatasetChanges(Resource):
    @api.marshal_with(dataset_changes_model)
    def get(self):
        """Get changes in dataset since last update"""
        current_info = get_dataset_info()
        global previous_dataset_info
        
        # If we don't have previous info, create .last_update file
        if not os.path.exists(os.path.join(BASE_DIR, 'output', '.last_update')):
            with open(os.path.join(BASE_DIR, 'output', '.last_update'), 'w') as f:
                f.write(datetime.now().isoformat())
        
        changes = calculate_dataset_changes(previous_dataset_info, current_info)
        return changes

def update_dataset_info():
    """Update the stored dataset info after ETL process"""
    global previous_dataset_info
    previous_dataset_info = get_dataset_info()

if __name__ == '__main__':
    app.run(debug=True, port=5002)
