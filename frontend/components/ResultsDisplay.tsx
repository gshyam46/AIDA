'use client'

import React from 'react'
import { CheckCircle, AlertCircle, Database, Code, Zap, Clock } from 'lucide-react'

interface QueryResult {
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

interface ResultsDisplayProps {
  result: QueryResult | null
}

export default function ResultsDisplay({ result }: ResultsDisplayProps) {
  if (!result) return null

  if (!result.success) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6">
        <div className="flex items-start gap-3">
          <AlertCircle className="w-6 h-6 text-red-600 mt-0.5 flex-shrink-0" />
          <div>
            <h3 className="font-semibold text-red-900 mb-2">Query Failed</h3>
            <p className="text-red-700 text-sm">{result.error}</p>
            <p className="text-red-600 text-xs mt-2">
              Execution time: {result.execution_time_ms}ms
            </p>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Final Results */}
      <div className="bg-white rounded-lg shadow-lg p-6">
        <div className="flex items-center gap-3 mb-4">
          <CheckCircle className="w-6 h-6 text-green-600" />
          <h3 className="text-lg font-semibold text-gray-900">Query Results</h3>
          <div className="flex items-center gap-1 text-sm text-gray-500 ml-auto">
            <Clock className="w-4 h-4" />
            {result.execution_time_ms}ms
          </div>
        </div>
        
        <div className="bg-green-50 border border-green-200 rounded-lg p-4">
          {result.results && result.results.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="min-w-full">
                <thead>
                  <tr className="border-b border-green-200">
                    {Object.keys(result.results[0]).map((key) => (
                      <th key={key} className="text-left py-2 px-3 font-medium text-green-900">
                        {key}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.results.map((row, index) => (
                    <tr key={index} className="border-b border-green-100">
                      {Object.values(row).map((value, cellIndex) => (
                        <td key={cellIndex} className="py-2 px-3 text-green-800">
                          {value !== null ? String(value) : 'null'}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-green-800 font-mono text-lg">No results found</p>
          )}
        </div>
      </div>

      {/* Pipeline Steps */}
      <div className="grid gap-6 md:grid-cols-2">
        {/* Semantic IR */}
        {result.semantic_ir && (
          <div className="bg-white rounded-lg shadow p-4">
            <div className="flex items-center gap-2 mb-3">
              <Zap className="w-5 h-5 text-purple-600" />
              <h4 className="font-semibold text-gray-900">1. Semantic Analysis</h4>
            </div>
            <pre className="bg-purple-50 p-3 rounded text-sm overflow-x-auto text-purple-800">
              {JSON.stringify(result.semantic_ir, null, 2)}
            </pre>
          </div>
        )}

        {/* Canonical IR */}
        {result.canonical_ir && (
          <div className="bg-white rounded-lg shadow p-4">
            <div className="flex items-center gap-2 mb-3">
              <Database className="w-5 h-5 text-indigo-600" />
              <h4 className="font-semibold text-gray-900">2. Business Logic</h4>
            </div>
            <pre className="bg-indigo-50 p-3 rounded text-sm overflow-x-auto text-indigo-800">
              {JSON.stringify(result.canonical_ir, null, 2)}
            </pre>
          </div>
        )}
      </div>

      {/* Generated SQL */}
      {result.sql && (
        <div className="bg-white rounded-lg shadow p-4">
          <div className="flex items-center gap-2 mb-3">
            <Code className="w-5 h-5 text-blue-600" />
            <h4 className="font-semibold text-gray-900">3. Generated SQL</h4>
          </div>
          <div className="bg-blue-50 p-4 rounded">
            <pre className="text-sm font-mono text-blue-800 overflow-x-auto">
              {result.sql}
            </pre>
            {result.parameters && Object.keys(result.parameters).length > 0 && (
              <div className="mt-3 pt-3 border-t border-blue-200">
                <p className="text-sm font-medium text-blue-900 mb-2">Parameters:</p>
                <pre className="text-xs font-mono text-blue-700">
                  {JSON.stringify(result.parameters, null, 2)}
                </pre>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}