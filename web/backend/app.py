from flask import Flask, jsonify, make_response
from flask_cors import CORS
from flask_caching import Cache
from flask_restx import Api, Resource, fields
import pandas as pd
import numpy as np
import json
import aiohttp
import asyncio
from pathlib import Path
import os
import datetime as dt
from datetime import datetime, timedelta
from tqdm import tqdm

app = Flask(__name__)
# Initialize Flask-RESTX
api = Api(app, version='1.0', 
    title='Catalonia Water Resource Monitoring API',
    description='API for monitoring water resources in Catalonia',
    doc='/'  # Swagger UI will be available at root URL
)

# Create namespaces for different API endpoints
ns_sensors = api.namespace('api/sensors', description='Sensor operations')
ns_dataset = api.namespace('api/dataset', description='Dataset operations')

# Define models for Swagger documentation
date_range_model = api.model('DateRange', {
    'start': fields.String(description='Start date of readings'),
    'end': fields.String(description='End date of readings')
})

sensor_summary_model = api.model('SensorSummary', {
    'total_sensors': fields.Integer(description='Total number of sensors'),
    'total_readings': fields.Integer(description='Total number of readings'),
    'date_range': fields.Nested(date_range_model)
})

sensor_data_model = api.model('SensorData', {
    'timestamps': fields.List(fields.String, description='List of timestamps'),
    'sensors': fields.Raw(description='Sensor readings keyed by sensor ID')
})

dataset_info_model = api.model('DatasetInfo', {
    'total_size': fields.String(description='Total size of dataset files'),
    'file_count': fields.Integer(description='Number of files in dataset'),
    'files': fields.List(fields.String, description='List of files in dataset')
})

# Configure CORS to allow all origins during development
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
    response.headers.add('Access-Control-Allow-Methods', 'GET,POST,OPTIONS')
    return response

# Configure Flask-Caching
cache = Cache(app, config={
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 300  # Cache for 5 minutes
})

# Data paths - using absolute paths
BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
DATA_DIR = BASE_DIR / "output" / "sensor_data"
METADATA_DIR = BASE_DIR / "output" / "metadata"

# Create output directories
DATA_DIR.mkdir(parents=True, exist_ok=True)
METADATA_DIR.mkdir(parents=True, exist_ok=True)

print(f"Looking for data in: {DATA_DIR}")
print(f"Looking for metadata in: {METADATA_DIR}")

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

@cache.memoize(timeout=3600)  # Cache for 1 hour
def load_data():
    data = {}
    metadata = {}
    sensor_types = ['reservoir', 'gauge', 'pluviometer', 'piezometer']
    
    # Load sensor data
    for sensor_type in sensor_types:
        try:
            data_file = DATA_DIR / f"{sensor_type}_sensors_reads.csv"
            print(f"Loading {sensor_type} data from: {data_file}")
            if data_file.exists():
                data[sensor_type] = pd.read_csv(
                    data_file,
                    parse_dates=[0],
                    index_col=0
                )
                print(f"Loaded {sensor_type} data with shape: {data[sensor_type].shape}")
            else:
                print(f"Warning: {sensor_type} data file does not exist at {data_file}")
                data[sensor_type] = pd.DataFrame()
        except FileNotFoundError as e:
            print(f"Warning: {sensor_type} data file not found - {e}")
            data[sensor_type] = pd.DataFrame()
        except Exception as e:
            print(f"Error loading {sensor_type} data: {str(e)}")
            data[sensor_type] = pd.DataFrame()
            
        try:
            metadata_file = METADATA_DIR / f"{sensor_type}_sensors_metadata.csv"
            print(f"Loading {sensor_type} metadata from: {metadata_file}")
            if metadata_file.exists():
                metadata[sensor_type] = pd.read_csv(metadata_file)
                print(f"Loaded {sensor_type} metadata with shape: {metadata[sensor_type].shape}")
            else:
                print(f"Warning: {sensor_type} metadata file does not exist at {metadata_file}")
                metadata[sensor_type] = pd.DataFrame()
        except FileNotFoundError as e:
            print(f"Warning: {sensor_type} metadata file not found - {e}")
            metadata[sensor_type] = pd.DataFrame()
        except Exception as e:
            print(f"Error loading {sensor_type} metadata: {str(e)}")
            metadata[sensor_type] = pd.DataFrame()
    
    return data, metadata

# Load data at startup
sensor_data, sensor_metadata = load_data()

@ns_sensors.route('/summary')
class SensorsSummary(Resource):
    @api.doc('get_sensors_summary',
             description='Get a summary of all sensor data')
    @api.marshal_with(sensor_summary_model, as_list=False)
    @cache.cached(timeout=300)  # Cache for 5 minutes
    def get(self):
        """Get summary information for all sensor types"""
        summary = {}
        for sensor_type, data in sensor_data.items():
            if not data.empty:
                summary[sensor_type] = {
                    'total_sensors': len(data.columns),
                    'total_readings': len(data),
                    'date_range': {
                        'start': data.index[0],
                        'end': data.index[-1]
                    }
                }
        return summary

@ns_sensors.route('/<string:sensor_type>/data')
@api.doc(params={'sensor_type': 'Type of sensor (reservoir, gauge, pluviometer, piezometer)'})
class SensorData(Resource):
    @api.doc('get_sensor_data',
             description='Get time series data for a specific sensor type')
    @api.marshal_with(sensor_data_model)
    @api.response(404, 'Sensor type not found')
    @cache.memoize(timeout=300)  # Cache for 5 minutes
    def get(self, sensor_type):
        """Get time series data for a specific sensor type"""
        if sensor_type not in sensor_data:
            api.abort(404, f"Sensor type '{sensor_type}' not found")
        
        df = sensor_data[sensor_type]
        if df.empty:
            api.abort(404, f"No data available for sensor type '{sensor_type}'")
        
        # Convert data to time series format
        data = {
            'timestamps': df.index.strftime('%Y-%m-%d %H:%M:%S').tolist(),
            'sensors': {
                col: df[col].replace({pd.NA: None, pd.NaT: None, np.nan: None}).tolist() 
                for col in df.columns
            }
        }
        return data

@ns_sensors.route('/<string:sensor_type>/metadata')
@api.doc(params={'sensor_type': 'Type of sensor (reservoir, gauge, pluviometer, piezometer)'})
class SensorMetadata(Resource):
    @api.doc('get_sensor_metadata',
             description='Get metadata for a specific sensor type')
    @api.response(404, 'Sensor type not found')
    @cache.memoize(timeout=3600)  # Cache for 1 hour
    def get(self, sensor_type):
        """Get metadata for a specific sensor type"""
        if sensor_type not in sensor_metadata:
            api.abort(404, f"Sensor type '{sensor_type}' not found")
        
        df = sensor_metadata[sensor_type]
        if df.empty:
            api.abort(404, f"No metadata available for sensor type '{sensor_type}'")
        
        return df.to_dict(orient='records')

@ns_sensors.route('/update-data')
class UpdateData(Resource):
    @api.doc('update_sensor_data',
             description='Update sensor data by fetching latest readings')
    def post(self):
        """Trigger an update of sensor data"""
        try:
            print("Starting data update...")
            # Prepare sensor IDs
            groups_cols_ids = []
            for sensor_type in ['reservoir', 'gauge', 'pluviometer', 'piezometer']:
                if sensor_type in sensor_metadata and not sensor_metadata[sensor_type].empty:
                    print(f"Getting sensor IDs for {sensor_type}")
                    cols = sensor_metadata[sensor_type]["sensor_id"].tolist()
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
            
            # Update data
            for sensor_type, df_sensor in zip(['reservoir', 'gauge', 'pluviometer', 'piezometer'], last_dfs_sensors_day):
                print(f"Processing {sensor_type} data...")
                if not df_sensor.empty:
                    # Save to file
                    output_file = DATA_DIR / f"{sensor_type}_sensors_reads.csv"
                    print(f"Saving {sensor_type} data to {output_file}")
                    df_sensor.to_csv(output_file)
                    print(f"Saved data with shape: {df_sensor.shape}")
                    
                    # Update in-memory data
                    sensor_data[sensor_type] = pd.concat([sensor_data[sensor_type], df_sensor], axis=0)
                    sensor_data[sensor_type] = sensor_data[sensor_type].sort_index(ascending=False)
                    sensor_data[sensor_type] = sensor_data[sensor_type][~sensor_data[sensor_type].index.duplicated(keep="first")]
                    print(f"Updated in-memory data for {sensor_type}, new shape: {sensor_data[sensor_type].shape}")
                else:
                    print(f"No new data for {sensor_type}")
            
            print("Data update completed successfully")
            return {'message': 'Data updated successfully'}
        except Exception as e:
            print(f"Error updating data: {str(e)}")
            import traceback
            traceback.print_exc()
            api.abort(500, str(e))

@ns_dataset.route('/info')
class DatasetInfo(Resource):
    @api.doc('get_dataset_info',
             description='Get information about the dataset folder')
    @api.marshal_with(dataset_info_model)
    @cache.cached(timeout=300)  # Cache for 5 minutes
    def get(self):
        """Get summary information about the dataset folder"""
        try:
            total_size = 0
            file_count = 0
            files = []
            
            # Count files in data and metadata directories
            for directory in [DATA_DIR, METADATA_DIR]:
                if directory.exists():
                    for file in directory.glob('*.csv'):
                        file_count += 1
                        size = file.stat().st_size
                        total_size += size
                        files.append(f"{file.name} ({size / 1024:.1f} KB)")
            
            return {
                'total_size': f"{total_size / 1024:.1f} KB",
                'file_count': file_count,
                'files': files
            }
        except Exception as e:
            api.abort(500, f"Error getting dataset info: {str(e)}")

if __name__ == '__main__':
    app.run(debug=True, port=5002)
