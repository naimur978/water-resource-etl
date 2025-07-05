import React, { useState, useEffect } from 'react';
import { getDatasetInfo, startETLProcess, getProcessedDatasetInfo } from './api';

function App() {
  // State for initial dataset info
  const [inputDataset, setInputDataset] = useState(null);
  const [inputLoading, setInputLoading] = useState(true);
  const [inputError, setInputError] = useState(null);

  // State for processed dataset info
  const [processedDataset, setProcessedDataset] = useState(null);
  const [processedLoading, setProcessedLoading] = useState(false);
  const [processedError, setProcessedError] = useState(null);

  // State for ETL processing
  const [processing, setProcessing] = useState(false);
  const [processError, setProcessError] = useState(null);

  // Fetch initial dataset info on component mount
  useEffect(() => {
    fetchInputDataset();
  }, []);

  const fetchInputDataset = async () => {
    try {
      setInputLoading(true);
      setInputError(null);
      const data = await getDatasetInfo();
      setInputDataset(data);
    } catch (err) {
      setInputError('Error fetching input dataset information');
      console.error('Error:', err);
    } finally {
      setInputLoading(false);
    }
  };

  const handleStartETL = async () => {
    try {
      setProcessing(true);
      setProcessError(null);
      setProcessedLoading(true);
      setProcessedError(null);

      // Start ETL process
      await startETLProcess();
      
      // Get processed dataset info
      const processedData = await getProcessedDatasetInfo();
      setProcessedDataset(processedData);
    } catch (err) {
      setProcessError('Error during ETL process');
      setProcessedError('Error fetching processed dataset information');
      console.error('Error:', err);
    } finally {
      setProcessing(false);
      setProcessedLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-100 py-8">
      <div className="max-w-4xl mx-auto px-4">
        <h1 className="text-3xl font-bold text-gray-900 text-center mb-8">
          Water Resource ETL Dashboard
        </h1>

        {/* Input Dataset Section */}
        <div className="mb-8">
          {inputLoading ? (
            <p>Loading dataset information...</p>
          ) : inputError ? (
            <p className="text-red-600">{inputError}</p>
          ) : inputDataset && (
            <div>
              <h2 className="text-xl font-semibold mb-4">Input Dataset (dataset/)</h2>
              <pre className="bg-white p-4 rounded shadow">
                {JSON.stringify(inputDataset, null, 2)}
              </pre>
            </div>
          )}
        </div>

        {/* ETL Process Button */}
        <div className="mb-8 text-center">
          <button
            onClick={handleStartETL}
            disabled={processing || inputLoading || !!inputError}
            className={`px-6 py-3 rounded-lg font-medium text-white ${
              processing || inputLoading || !!inputError
                ? 'bg-gray-400'
                : 'bg-blue-600 hover:bg-blue-700'
            }`}
          >
            {processing ? 'Processing...' : 'Start ETL Process'}
          </button>
          {processError && (
            <p className="mt-2 text-red-600">{processError}</p>
          )}
        </div>

        {/* Processed Dataset Section */}
        {(processedDataset || processedLoading || processedError) && (
          <div className="mb-8">
            {processedLoading ? (
              <p>Loading processed dataset information...</p>
            ) : processedError ? (
              <p className="text-red-600">{processedError}</p>
            ) : processedDataset && (
              <div>
                <h2 className="text-xl font-semibold mb-4">Processed Dataset (output/)</h2>
                <pre className="bg-white p-4 rounded shadow">
                  {JSON.stringify(processedDataset, null, 2)}
                </pre>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
