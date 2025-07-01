import React from 'react';

const DatasetChanges = ({ datasetInfo }) => {
  if (!datasetInfo) return null;

  return (
    <div className="min-h-screen flex items-center justify-center">
      <div className="bg-white rounded-lg shadow-lg p-8 text-center">
        <h2 className="text-3xl font-bold mb-8 text-gray-900">Dataset Overview</h2>
        
        {/* Stats Section */}
        <div className="space-y-6 mb-8">
          <div className="p-6 bg-gray-50 rounded-lg">
            <h3 className="text-lg font-semibold text-gray-700 mb-2">Total Size</h3>
            <p className="text-3xl font-bold text-blue-600">{datasetInfo.total_size}</p>
          </div>
          <div className="p-6 bg-gray-50 rounded-lg">
            <h3 className="text-lg font-semibold text-gray-700 mb-2">Total Files</h3>
            <p className="text-3xl font-bold text-green-600">{datasetInfo.file_count}</p>
          </div>
        </div>

        {/* File List Section */}
        <div>
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Files in Dataset</h3>
          <div className="bg-gray-50 rounded-lg p-4 max-h-48 overflow-y-auto">
            <div className="space-y-2">
              {datasetInfo.files.map((file) => (
                <div key={file} className="px-3 py-2 text-sm text-gray-700 bg-white rounded text-center">
                  {file}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DatasetChanges;
