import { useState, useEffect } from 'react';
import axios from 'axios';
import DatasetChanges from './components/DatasetChanges';
import ETLProcessor from './components/ETLProcessor';

function App() {
  const [datasetInfo, setDatasetInfo] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetchDatasetInfo = async () => {
    try {
      setLoading(true);
      const response = await axios.get('http://localhost:5004/dataset/info');
      setDatasetInfo(response.data);
      setError(null);
    } catch (err) {
      setError('Error fetching dataset information');
      console.error('Error:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDatasetInfo();
  }, []);

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-100 flex items-center justify-center">
        <div className="text-center">
          <p className="text-lg text-gray-600">Loading dataset information...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-100 flex items-center justify-center">
        <div className="max-w-md mx-auto">
          <div className="bg-red-50 border border-red-200 rounded-md p-4 text-center">
            <p className="text-red-800">{error}</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-100">
      <div className="container mx-auto px-4 py-8">
        <h1 className="text-3xl font-bold text-gray-900 text-center mb-8">
          Water Resource ETL Dashboard
        </h1>
        
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 max-w-6xl mx-auto">
          {/* Dataset Overview */}
          <div>
            <DatasetChanges datasetInfo={datasetInfo} />
          </div>
          
          {/* ETL Processing */}
          <div>
            <ETLProcessor onProcessComplete={fetchDatasetInfo} />
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
