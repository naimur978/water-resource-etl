function DatasetInfo({ title, data, isLoading, error }) {
  if (isLoading) {
    return (
      <div className="p-4 bg-white rounded-lg shadow">
        <p className="text-gray-600">Loading dataset information...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-4 bg-red-50 border border-red-200 rounded-lg">
        <p className="text-red-600">{error}</p>
      </div>
    );
  }

  if (!data) {
    return null;
  }

  return (
    <div className="p-4 bg-white rounded-lg shadow">
      <h2 className="text-xl font-semibold mb-4">{title}</h2>
      <div className="space-y-4">
        <div>
          <p className="text-gray-600">Total Size:</p>
          <p className="font-medium">{data.total_size}</p>
        </div>
        <div>
          <p className="text-gray-600">Number of Files:</p>
          <p className="font-medium">{data.file_count}</p>
        </div>
        <div>
          <p className="text-gray-600 mb-2">Files:</p>
          <ul className="list-disc pl-5 space-y-1">
            {data.files.map((file, index) => (
              <li key={index} className="text-sm text-gray-700">{file}</li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
}

export default DatasetInfo;
