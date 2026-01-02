'use client'

import React, { useState, useEffect } from 'react'
import QueryInterface from '../components/QueryInterface'
import ResultsDisplay from '../components/ResultsDisplay'
import SchemaDisplay from '../components/SchemaDisplay'
import FileUpload from '../components/FileUpload'
import { 
  executeQuery, 
  getSchema, 
  uploadDatabase, 
  getExamples,
  type QueryResponse,
  type SchemaResponse,
  type ExampleResponse
} from '../lib/api'

export default function Home() {
  const [queryResult, setQueryResult] = useState<QueryResponse | null>(null)
  const [schema, setSchema] = useState<SchemaResponse | null>(null)
  const [examples, setExamples] = useState<string[]>([])
  const [loading, setLoading] = useState({
    query: false,
    schema: false,
    upload: false
  })

  // Load initial data
  useEffect(() => {
    loadInitialData()
  }, [])

  const loadInitialData = async () => {
    try {
      // Load schema
      setLoading(prev => ({ ...prev, schema: true }))
      const schemaData = await getSchema()
      setSchema(schemaData)

      // Load examples
      const exampleData = await getExamples()
      setExamples(exampleData.examples.map(ex => ex.question))
    } catch (error) {
      console.error('Failed to load initial data:', error)
    } finally {
      setLoading(prev => ({ ...prev, schema: false }))
    }
  }

  const handleQuerySubmit = async (question: string) => {
    try {
      setLoading(prev => ({ ...prev, query: true }))
      setQueryResult(null)
      
      const result = await executeQuery({ question })
      setQueryResult(result)
    } catch (error) {
      console.error('Query failed:', error)
      setQueryResult({
        success: false,
        question,
        execution_time_ms: 0,
        error: error instanceof Error ? error.message : 'Query failed'
      })
    } finally {
      setLoading(prev => ({ ...prev, query: false }))
    }
  }

  const handleFileUpload = async (file: File) => {
    try {
      setLoading(prev => ({ ...prev, upload: true }))
      
      const uploadResult = await uploadDatabase(file)
      
      // Refresh schema after successful upload
      const newSchema = await getSchema(uploadResult.file_path)
      setSchema(newSchema)
      
      // Clear previous query results
      setQueryResult(null)
    } catch (error) {
      console.error('Upload failed:', error)
      throw error
    } finally {
      setLoading(prev => ({ ...prev, upload: false }))
    }
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 py-8">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {/* Main Query Interface - Left Column */}
          <div className="lg:col-span-2 space-y-6">
            <QueryInterface
              onSubmit={handleQuerySubmit}
              loading={loading.query}
              examples={examples}
            />
            
            <ResultsDisplay result={queryResult} />
          </div>

          {/* Sidebar - Right Column */}
          <div className="space-y-6">
            <FileUpload
              onUpload={handleFileUpload}
              loading={loading.upload}
            />
            
            <SchemaDisplay
              schema={schema}
              loading={loading.schema}
            />
          </div>
        </div>

        {/* Footer */}
        <div className="mt-12 bg-blue-50 border border-blue-200 rounded-lg p-6">
          <h3 className="font-semibold text-blue-900 mb-3">How it works</h3>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 text-sm text-blue-800">
            <div className="flex items-start gap-2">
              <div className="w-6 h-6 bg-blue-600 text-white rounded-full flex items-center justify-center text-xs font-bold">
                1
              </div>
              <div>
                <p className="font-medium">Natural Language</p>
                <p>Ask questions in plain English</p>
              </div>
            </div>
            <div className="flex items-start gap-2">
              <div className="w-6 h-6 bg-blue-600 text-white rounded-full flex items-center justify-center text-xs font-bold">
                2
              </div>
              <div>
                <p className="font-medium">AI Analysis</p>
                <p>LLM understands your intent</p>
              </div>
            </div>
            <div className="flex items-start gap-2">
              <div className="w-6 h-6 bg-blue-600 text-white rounded-full flex items-center justify-center text-xs font-bold">
                3
              </div>
              <div>
                <p className="font-medium">Safe SQL</p>
                <p>Generate secure, read-only queries</p>
              </div>
            </div>
            <div className="flex items-start gap-2">
              <div className="w-6 h-6 bg-blue-600 text-white rounded-full flex items-center justify-center text-xs font-bold">
                4
              </div>
              <div>
                <p className="font-medium">Results</p>
                <p>Get your data with full transparency</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}