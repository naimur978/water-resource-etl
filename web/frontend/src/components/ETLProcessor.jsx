import React, { useState } from 'react';
import axios from 'axios';

const ETLProcessor = ({ onProcessComplete }) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [progress, setProgress] = useState(0);
  const [currentStep, setCurrentStep] = useState('');
  const [datasetChanges, setDatasetChanges] = useState(null);
  const [showResults, setShowResults] = useState(false);

  const handleETLProcess = async () => {
    try {
      setLoading(true);
      setError(null);
      setSuccess(false);
      setProgress(0);
      
      // Simulate progress steps
      setCurrentStep('Initializing ETL process...');
      setProgress(10);
      await new Promise(resolve => setTimeout(resolve, 500));
      
      setCurrentStep('Connecting to data sources...');
      setProgress(25);
      await new Promise(resolve => setTimeout(resolve, 500));
      
      setCurrentStep('Fetching sensor data...');
      setProgress(50);
      
      const response = await axios.post('http://localhost:5004/sensors/update-data');
      
      setCurrentStep('Processing data...');
      setProgress(75);
      await new Promise(resolve => setTimeout(resolve, 500));
      
      setCurrentStep('Finalizing updates...');
      setProgress(90);
      await new Promise(resolve => setTimeout(resolve, 500));
      
      setProgress(100);
      setCurrentStep('Complete!');
      setSuccess(true);
      setLastUpdated(new Date().toLocaleString());
      
      // Fetch dataset changes after ETL completion
      try {
        const changesResponse = await axios.get('http://localhost:5004/dataset/changes');
        setDatasetChanges(changesResponse.data);
        setShowResults(true);
      } catch (changesErr) {
        console.error('Error fetching dataset changes:', changesErr);
      }
      
      // Call the callback to refresh dataset info
      if (onProcessComplete) {
        onProcessComplete();
      }
      
      // Clear success message after 10 seconds
      setTimeout(() => {
        setSuccess(false);
        setProgress(0);
        setCurrentStep('');
        setShowResults(false);
        setDatasetChanges(null);
      }, 10000);
      
    } catch (err) {
      setError('Failed to process ETL. Please try again.');
      setProgress(0);
      setCurrentStep('');
      setShowResults(false);
      setDatasetChanges(null);
      console.error('ETL Error:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="bg-white rounded-lg shadow-lg p-6">
      <h3 className="text-xl font-semibold text-gray-900 mb-4">ETL Processing</h3>
      <p className="text-gray-600 mb-6">
        Update sensor data by fetching the latest readings from all monitoring stations.
      </p>
      
      {/* ETL Button */}
      <div className="flex flex-col items-center space-y-4">
        <button
          onClick={handleETLProcess}
          disabled={loading}
          className={`
            px-6 py-3 rounded-lg font-medium text-white transition-all duration-200
            ${loading 
              ? 'bg-gray-400 cursor-not-allowed' 
              : 'bg-blue-600 hover:bg-blue-700 active:bg-blue-800 shadow-md hover:shadow-lg'
            }
          `}
        >
          {loading ? (
            <div className="flex items-center space-x-2">
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
              <span>Processing... {progress}%</span>
            </div>
          ) : (
            'Run ETL Process'
          )}
        </button>
        
        {/* Progress Bar */}
        {loading && (
          <div className="w-full space-y-2">
            <div className="flex justify-between text-sm text-gray-600">
              <span>{currentStep}</span>
              <span>{progress}%</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2">
              <div 
                className="bg-blue-600 h-2 rounded-full transition-all duration-500 ease-out"
                style={{ width: `${progress}%` }}
              ></div>
            </div>
          </div>
        )}
        
        {/* Status Messages */}
        {error && (
          <div className="w-full p-3 bg-red-50 border border-red-200 rounded-lg">
            <div className="flex items-center">
              <svg className="w-4 h-4 text-red-500 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              <span className="text-sm text-red-700">{error}</span>
            </div>
          </div>
        )}
        
        {success && (
          <div className="w-full p-3 bg-green-50 border border-green-200 rounded-lg">
            <div className="flex items-center">
              <svg className="w-4 h-4 text-green-500 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M5 13l4 4L19 7" />
              </svg>
              <span className="text-sm text-green-700">ETL process completed successfully!</span>
            </div>
          </div>
        )}
        
        {lastUpdated && (
          <div className="text-xs text-gray-500">
            Last updated: {lastUpdated}
          </div>
        )}
        
        {/* Dataset Changes Results */}
        {showResults && datasetChanges && (
          <div className="w-full mt-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
            <h4 className="text-lg font-semibold text-blue-900 mb-3">Output Folder Changes</h4>
            
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
              <div className="text-center p-3 bg-white rounded-lg">
                <div className="text-sm text-gray-600">Size Change</div>
                <div className="text-lg font-bold text-blue-600">{datasetChanges.size_change}</div>
              </div>
              <div className="text-center p-3 bg-white rounded-lg">
                <div className="text-sm text-gray-600">Added Files</div>
                <div className="text-lg font-bold text-green-600">{datasetChanges.added_files.length}</div>
              </div>
              <div className="text-center p-3 bg-white rounded-lg">
                <div className="text-sm text-gray-600">Modified Files</div>
                <div className="text-lg font-bold text-orange-600">{datasetChanges.modified_files.length}</div>
              </div>
            </div>
            
            {datasetChanges.added_files.length > 0 && (
              <div className="mb-3">
                <h5 className="text-sm font-medium text-green-800 mb-2">Added Files:</h5>
                <div className="max-h-24 overflow-y-auto">
                  {datasetChanges.added_files.map((file, index) => (
                    <div key={index} className="text-xs text-green-700 bg-green-50 px-2 py-1 rounded mb-1">
                      + {file}
                    </div>
                  ))}
                </div>
              </div>
            )}
            
            {datasetChanges.modified_files.length > 0 && (
              <div className="mb-3">
                <h5 className="text-sm font-medium text-orange-800 mb-2">Modified Files:</h5>
                <div className="max-h-24 overflow-y-auto">
                  {datasetChanges.modified_files.map((file, index) => (
                    <div key={index} className="text-xs text-orange-700 bg-orange-50 px-2 py-1 rounded mb-1">
                      ~ {file}
                    </div>
                  ))}
                </div>
              </div>
            )}
            
            {datasetChanges.current_info && (
              <div className="pt-3 border-t border-blue-200">
                <h5 className="text-sm font-medium text-blue-800 mb-2">Current Output Folder:</h5>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div className="text-gray-600">
                    <span className="font-medium">Total Size:</span> {datasetChanges.current_info.total_size}
                  </div>
                  <div className="text-gray-600">
                    <span className="font-medium">Total Files:</span> {datasetChanges.current_info.file_count}
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
      
      {/* Process Info */}
      <div className="mt-6 pt-4 border-t border-gray-200">
        <h4 className="text-sm font-medium text-gray-900 mb-2">What this process does:</h4>
        <ul className="text-xs text-gray-600 space-y-1">
          <li>• Fetches latest sensor readings from all monitoring stations</li>
          <li>• Updates reservoir, gauge, pluviometer, and piezometer data</li>
          <li>• Processes and validates the collected data</li>
          <li>• Updates the dataset files with new information</li>
        </ul>
      </div>
    </div>
  );
};

export default ETLProcessor;
