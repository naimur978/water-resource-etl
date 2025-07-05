import axios from 'axios';

const API_BASE_URL = 'http://localhost:5002';

// Get current dataset information (input folder)
export async function getDatasetInfo() {
  const response = await axios.get(`${API_BASE_URL}/dataset/info`);
  return response.data;
}

// Start ETL process (merge/update data)
export async function startETLProcess() {
  const response = await axios.post(`${API_BASE_URL}/sensors/update-data`);
  return response.data;
}

// Get dataset changes after ETL process (output folder)
export async function getProcessedDatasetInfo() {
  const response = await axios.get(`${API_BASE_URL}/dataset/processed/info`);
  return response.data;
}
