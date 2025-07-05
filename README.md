# water-resource-etl

## Live Demo
The application is deployed and can be accessed at:
[Water Resource ETL App](https://water-resource-kazsqhciy-naimurs-projects-0139012a.vercel.app/)

## Project Overview
This project is an ETL (Extract, Transform, Load) application for water resource data, designed to experiment with real-time and historical sensor data. The purpose of this project is to have some fun with existing data while handling new data that comes in every day, running ETL processes to transform and visualize the information.

The application processes data from various water-related sensors including:

## Data Sources
The following official sources provide daily updated data, which is automatically merged with historical data in this project:
- Reservoir data: [EMBASSAMENT-EST](https://aplicacions.aca.gencat.cat/sdim2/apirest/data/EMBASSAMENT-EST)
- Gauge data: [AFORAMENT-EST](https://aplicacions.aca.gencat.cat/sdim2/apirest/data/AFORAMENT-EST)
- Pluviometer data: [PLUVIOMETREACA-EST](https://aplicacions.aca.gencat.cat/sdim2/apirest/data/PLUVIOMETREACA-EST)
- Piezometer data: [PIEZOMETRE-EST](https://aplicacions.aca.gencat.cat/sdim2/apirest/data/PIEZOMETRE-EST)
## Project Structure
```
water-resource-etl/
├── dataset/               # Raw sensor data
│   ├── gauge_sensors_reads.csv
│   ├── piezometer_sensors_reads.csv
│   ├── pluviometer_sensors_reads.csv
│   ├── reservoir_sensors_reads.csv
│   └── metadata/         # Sensor metadata
├── output/               # Processed data
│   ├── metadata/
│   └── sensor_data/
└── web/                  # Web application
    ├── backend/         # Flask backend
    └── frontend/        # React frontend
```

## Technologies Used
- Frontend: React with Vite
- Backend: Flask
- Data Processing: Python
- Deployment:
  - Frontend: Vercel
  - Backend: Render

## Getting Started
1. Clone the repository
2. Install dependencies:
   - Backend: `pip install -r requirements.txt`
   - Frontend: `npm install`
3. Start the development servers:
   - Backend: `python web/backend/app.py`
   - Frontend: `cd web/frontend && npm run dev`

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details