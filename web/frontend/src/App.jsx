import { useEffect, useState } from 'react';
import axios from 'axios';
import { Chart as ChartJS, ArcElement, Tooltip, Legend } from 'chart.js';
import { Pie } from 'react-chartjs-2';

ChartJS.register(ArcElement, Tooltip, Legend);

function App() {
  const [datasetInfo, setDatasetInfo] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchDatasetInfo = async () => {
      try {
        setLoading(true);
        const response = await axios.get('http://localhost:5002/api/dataset/info');
        setDatasetInfo(response.data);
        setError(null);
      } catch (err) {
        setError('Error fetching dataset information');
        console.error('Error:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchDatasetInfo();
  }, []);

  const chartData = datasetInfo ? {
    labels: ['Sensor Data Files', 'Metadata Files'],
    datasets: [{
      data: [
        datasetInfo.files.filter(f => !f.includes('metadata')).length,
        datasetInfo.files.filter(f => f.includes('metadata')).length,
      ],
      backgroundColor: ['rgba(54, 162, 235, 0.6)', 'rgba(75, 192, 192, 0.6)'],
      borderColor: ['rgba(54, 162, 235, 1)', 'rgba(75, 192, 192, 1)'],
      borderWidth: 1,
    }]
  } : null;

  if (loading) {
    return (
      <div className="container mx-auto px-4 py-8 max-w-7xl">
        <div className="p-4 bg-gray-100 rounded-lg">Loading dataset info...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container mx-auto px-4 py-8 max-w-7xl">
        <div className="p-4 bg-red-100 text-red-700 rounded-lg">{error}</div>
      </div>
    );
  }

  if (!datasetInfo) {
    return null;
  }

  return (
    <div className="container mx-auto px-4 py-8 max-w-7xl">
      <h1 className="text-3xl font-bold text-center mb-8 text-gray-800">
        Water Resource Monitoring Dataset
      </h1>

      <div className="bg-white p-6 rounded-lg shadow-lg mb-6">
        <h2 className="text-xl font-semibold mb-4 text-gray-800">Dataset Overview</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          <div className="bg-blue-50 p-4 rounded-lg">
            <h3 className="font-medium text-blue-800">Total Storage</h3>
            <p className="text-2xl font-bold text-blue-600">{datasetInfo.total_size}</p>
          </div>
          <div className="bg-green-50 p-4 rounded-lg">
            <h3 className="font-medium text-green-800">Total Files</h3>
            <p className="text-2xl font-bold text-green-600">{datasetInfo.file_count}</p>
          </div>
          <div className="bg-purple-50 p-4 rounded-lg">
            <h3 className="font-medium text-purple-800">Last Update</h3>
            <p className="text-2xl font-bold text-purple-600">
              {new Date().toLocaleDateString()}
            </p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div>
            <h3 className="font-medium mb-2 text-gray-700">Data Distribution</h3>
            <div className="bg-white p-4 rounded-lg" style={{ height: '300px' }}>
              {chartData && <Pie data={chartData} options={{ maintainAspectRatio: false }} />}
            </div>
          </div>

          <div>
            <h3 className="font-medium mb-2 text-gray-700">Available Files</h3>
            <div className="bg-gray-50 p-4 rounded-lg max-h-[300px] overflow-y-auto">
              <div className="mb-4">
                <h4 className="text-sm font-medium text-blue-800 mb-2">Sensor Data Files:</h4>
                <ul className="list-disc list-inside space-y-1">
                  {datasetInfo.files
                    .filter(f => !f.includes('metadata'))
                    .map((file, index) => (
                      <li key={`data-${index}`} className="text-sm text-gray-600 pl-2">
                        {file}
                      </li>
                    ))}
                </ul>
              </div>
              <div>
                <h4 className="text-sm font-medium text-green-800 mb-2">Metadata Files:</h4>
                <ul className="list-disc list-inside space-y-1">
                  {datasetInfo.files
                    .filter(f => f.includes('metadata'))
                    .map((file, index) => (
                      <li key={`meta-${index}`} className="text-sm text-gray-600 pl-2">
                        {file}
                      </li>
                    ))}
                </ul>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="bg-white p-6 rounded-lg shadow-lg">
        <h2 className="text-xl font-semibold mb-4 text-gray-800">Dataset Structure</h2>
        <div className="prose max-w-none">
          <p className="text-gray-600 mb-4">
            This dataset contains water resource monitoring data from various sensors across Catalonia:
          </p>
          <ul className="list-disc list-inside space-y-2 text-gray-600">
            <li><span className="font-medium">Sensor Data Files:</span> Contains time series readings from different types of sensors (reservoir, gauge, pluviometer, piezometer)</li>
            <li><span className="font-medium">Metadata Files:</span> Contains sensor information including locations, units, and sensor specifications</li>
            <li><span className="font-medium">Data Format:</span> All files are in CSV format for easy processing and analysis</li>
            <li><span className="font-medium">Update Frequency:</span> Data is updated daily with the latest sensor readings</li>
          </ul>
        </div>
      </div>
    </div>
  );
}

export default App;
