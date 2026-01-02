'use client'

import React, { useRef, useState } from 'react'
import { Upload, Database, CheckCircle, AlertCircle, Loader2 } from 'lucide-react'

interface FileUploadProps {
  onUpload: (file: File) => Promise<void>
  loading: boolean
}

interface UploadStatus {
  type: 'success' | 'error' | null
  message: string
}

export default function FileUpload({ onUpload, loading }: FileUploadProps) {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [dragActive, setDragActive] = useState(false)
  const [uploadStatus, setUploadStatus] = useState<UploadStatus>({ type: null, message: '' })

  const handleFileSelect = async (file: File) => {
    if (!file.name.endsWith('.sqlite') && !file.name.endsWith('.db')) {
      setUploadStatus({
        type: 'error',
        message: 'Please select a SQLite database file (.sqlite or .db)'
      })
      return
    }

    try {
      setUploadStatus({ type: null, message: '' })
      await onUpload(file)
      setUploadStatus({
        type: 'success',
        message: `Successfully uploaded ${file.name}`
      })
    } catch (error) {
      setUploadStatus({
        type: 'error',
        message: error instanceof Error ? error.message : 'Upload failed'
      })
    }
  }

  const handleFileInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file) {
      handleFileSelect(file)
    }
  }

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    if (e.type === 'dragenter' || e.type === 'dragover') {
      setDragActive(true)
    } else if (e.type === 'dragleave') {
      setDragActive(false)
    }
  }

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setDragActive(false)

    const file = e.dataTransfer.files?.[0]
    if (file) {
      handleFileSelect(file)
    }
  }

  const handleClick = () => {
    fileInputRef.current?.click()
  }

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <div className="flex items-center gap-3 mb-4">
        <Database className="w-6 h-6 text-green-600" />
        <h3 className="text-lg font-semibold text-gray-900">Upload Database</h3>
      </div>

      <div
        className={`
          border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-colors
          ${dragActive 
            ? 'border-blue-400 bg-blue-50' 
            : 'border-gray-300 hover:border-gray-400 hover:bg-gray-50'
          }
          ${loading ? 'pointer-events-none opacity-50' : ''}
        `}
        onDragEnter={handleDrag}
        onDragLeave={handleDrag}
        onDragOver={handleDrag}
        onDrop={handleDrop}
        onClick={handleClick}
      >
        <input
          ref={fileInputRef}
          type="file"
          accept=".sqlite,.db"
          onChange={handleFileInputChange}
          className="hidden"
          disabled={loading}
        />

        <div className="flex flex-col items-center gap-3">
          {loading ? (
            <Loader2 className="w-12 h-12 text-blue-600 animate-spin" />
          ) : (
            <Upload className="w-12 h-12 text-gray-400" />
          )}
          
          <div>
            <p className="text-lg font-medium text-gray-900 mb-1">
              {loading ? 'Uploading...' : 'Upload SQLite Database'}
            </p>
            <p className="text-sm text-gray-600">
              Drag and drop your .sqlite or .db file here, or click to browse
            </p>
          </div>

          {!loading && (
            <button
              type="button"
              className="bg-green-600 hover:bg-green-700 text-white font-medium py-2 px-4 rounded-lg transition-colors"
            >
              Choose File
            </button>
          )}
        </div>
      </div>

      {uploadStatus.type && (
        <div className={`
          mt-4 p-3 rounded-lg flex items-start gap-3
          ${uploadStatus.type === 'success' 
            ? 'bg-green-50 border border-green-200' 
            : 'bg-red-50 border border-red-200'
          }
        `}>
          {uploadStatus.type === 'success' ? (
            <CheckCircle className="w-5 h-5 text-green-600 mt-0.5 flex-shrink-0" />
          ) : (
            <AlertCircle className="w-5 h-5 text-red-600 mt-0.5 flex-shrink-0" />
          )}
          <p className={`text-sm ${
            uploadStatus.type === 'success' ? 'text-green-800' : 'text-red-800'
          }`}>
            {uploadStatus.message}
          </p>
        </div>
      )}

      <div className="mt-4 text-xs text-gray-500">
        <p>Supported formats: SQLite database files (.sqlite, .db)</p>
        <p>Maximum file size: 100MB</p>
      </div>
    </div>
  )
}