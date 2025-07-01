import React, { useState } from 'react';
import axios from 'axios';

const ETLControl = ({ onComplete }) => {
  const [isRunning, setIsRunning] = useState(false);
  const [progress, setProgress] = useState(0);
  const [status, setStatus] = useState('');
  const [error, setError] = useState(null);

  const runETL = async () => {
    try {
      setIsRunning(true);
      setProgress(0);
      setError(null);
      setStatus('Starting ETL process...');

      // Start the ETL process
      await axios.post('http://localhost:5000/api/sensors/update-data');
      
      setStatus('ETL process completed successfully');
      setProgress(100);
      
      // Get the dataset changes
      const changesResponse = await axios.get('http://localhost:5000/api/dataset/changes');
      
      if (onComplete) {
        onComplete(changesResponse.data);
      }
    } catch (err) {
      setError(err.message);
      setStatus('Error occurred during ETL process');
    } finally {
      setIsRunning(false);
    }
  };

  return (
    <div className="mt-8">
      <button
        onClick={runETL}
        disabled={isRunning}
        className={`px-6 py-2 rounded-lg font-medium ${
          isRunning
            ? 'bg-gray-400 cursor-not-allowed'
            : 'bg-blue-600 hover:bg-blue-700 text-white'
        }`}
      >
        {isRunning ? 'Running ETL...' : 'Start ETL Process'}
      </button>

      {isRunning && (
        <div className="mt-4">
          <div className="w-full bg-gray-200 rounded-full h-2.5">
            <div
              className="bg-blue-600 h-2.5 rounded-full transition-all duration-500"
              style={{ width: `${progress}%` }}
            ></div>
          </div>
        </div>
      )}

      {status && (
        <p className={`mt-2 ${error ? 'text-red-600' : 'text-gray-600'}`}>
          {status}
        </p>
      )}
    </div>
  );
};

export default ETLControl;
