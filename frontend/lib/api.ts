import axios from 'axios'

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api/v1'

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 60000, // 60 seconds timeout
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor for logging
api.interceptors.request.use(
  (config) => {
    console.log(`API Request: ${config.method?.toUpperCase()} ${config.url}`)
    return config
  },
  (error) => {
    console.error('API Request Error:', error)
    return Promise.reject(error)
  }
)

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => {
    console.log(`API Response: ${response.status} ${response.config.url}`)
    return response
  },
  (error) => {
    console.error('API Response Error:', error.response?.data || error.message)
    return Promise.reject(error)
  }
)

export interface QueryRequest {
  question: string
  database_path?: string
}

export interface QueryResponse {
  success: boolean
  question: string
  semantic_ir?: any
  canonical_ir?: any
  sql?: string
  parameters?: any
  results?: any[]
  execution_time_ms: number
  error?: string
}

export interface SchemaResponse {
  success: boolean
  tables: Record<string, TableInfo>
  total_tables: number
  database_path?: string
  error?: string
}

export interface TableInfo {
  name: string
  columns: ColumnInfo[]
  row_count?: number
}

export interface ColumnInfo {
  name: string
  type: string
  nullable: boolean
  primary_key: boolean
  default_value?: string
}

export interface UploadResponse {
  success: boolean
  message: string
  file_path: string
  tables: string[]
  table_count: number
}

export interface ExampleResponse {
  examples: Array<{
    question: string
    description: string
  }>
}

// API functions
export const executeQuery = async (request: QueryRequest): Promise<QueryResponse> => {
  const response = await api.post<QueryResponse>('/query', request)
  return response.data
}

export const getSchema = async (databasePath?: string): Promise<SchemaResponse> => {
  const params = databasePath ? { database_path: databasePath } : {}
  const response = await api.get<SchemaResponse>('/schema', { params })
  return response.data
}

export const uploadDatabase = async (file: File): Promise<UploadResponse> => {
  const formData = new FormData()
  formData.append('file', file)
  
  const response = await api.post<UploadResponse>('/upload', formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
  })
  return response.data
}

export const getExamples = async (): Promise<ExampleResponse> => {
  const response = await api.get<ExampleResponse>('/examples')
  return response.data
}

export const healthCheck = async (): Promise<{ status: string }> => {
  const response = await api.get('/health')
  return response.data
}

export default api