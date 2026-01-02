'use client'

import React from 'react'
import { Database, Table, Hash, Type, Key } from 'lucide-react'

interface ColumnInfo {
  name: string
  type: string
  nullable: boolean
  primary_key: boolean
  default_value?: string
}

interface TableInfo {
  name: string
  columns: ColumnInfo[]
  row_count?: number
}

interface SchemaInfo {
  tables: Record<string, TableInfo>
  total_tables: number
  database_path?: string
}

interface SchemaDisplayProps {
  schema: SchemaInfo | null
  loading: boolean
}

export default function SchemaDisplay({ schema, loading }: SchemaDisplayProps) {
  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="animate-pulse">
          <div className="h-6 bg-gray-200 rounded w-1/3 mb-4"></div>
          <div className="space-y-3">
            <div className="h-4 bg-gray-200 rounded w-1/2"></div>
            <div className="h-4 bg-gray-200 rounded w-2/3"></div>
            <div className="h-4 bg-gray-200 rounded w-1/4"></div>
          </div>
        </div>
      </div>
    )
  }

  if (!schema || schema.total_tables === 0) {
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-6">
        <div className="text-center">
          <Database className="w-12 h-12 text-gray-400 mx-auto mb-3" />
          <p className="text-gray-600">No database schema available</p>
          <p className="text-sm text-gray-500 mt-1">Upload a database file to see its structure</p>
        </div>
      </div>
    )
  }

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <div className="flex items-center gap-3 mb-4">
        <Database className="w-6 h-6 text-blue-600" />
        <h3 className="text-lg font-semibold text-gray-900">Database Schema</h3>
        <span className="bg-blue-100 text-blue-800 text-sm px-2 py-1 rounded">
          {schema.total_tables} tables
        </span>
      </div>

      {schema.database_path && (
        <p className="text-sm text-gray-600 mb-4">
          Database: {schema.database_path}
        </p>
      )}

      <div className="space-y-4">
        {Object.values(schema.tables).map((table) => (
          <div key={table.name} className="border border-gray-200 rounded-lg p-4">
            <div className="flex items-center gap-2 mb-3">
              <Table className="w-5 h-5 text-green-600" />
              <h4 className="font-semibold text-gray-900">{table.name}</h4>
              {table.row_count !== undefined && (
                <span className="bg-gray-100 text-gray-700 text-xs px-2 py-1 rounded">
                  {table.row_count.toLocaleString()} rows
                </span>
              )}
            </div>

            <div className="overflow-x-auto">
              <table className="min-w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200">
                    <th className="text-left py-2 px-3 font-medium text-gray-700">Column</th>
                    <th className="text-left py-2 px-3 font-medium text-gray-700">Type</th>
                    <th className="text-left py-2 px-3 font-medium text-gray-700">Constraints</th>
                  </tr>
                </thead>
                <tbody>
                  {table.columns.map((column) => (
                    <tr key={column.name} className="border-b border-gray-100">
                      <td className="py-2 px-3 font-mono text-gray-900">
                        <div className="flex items-center gap-2">
                          {column.primary_key && <Key className="w-3 h-3 text-yellow-600" />}
                          {column.name}
                        </div>
                      </td>
                      <td className="py-2 px-3">
                        <div className="flex items-center gap-1">
                          <Type className="w-3 h-3 text-blue-600" />
                          <span className="font-mono text-blue-800">{column.type}</span>
                        </div>
                      </td>
                      <td className="py-2 px-3">
                        <div className="flex gap-1">
                          {column.primary_key && (
                            <span className="bg-yellow-100 text-yellow-800 text-xs px-1 py-0.5 rounded">
                              PK
                            </span>
                          )}
                          {!column.nullable && (
                            <span className="bg-red-100 text-red-800 text-xs px-1 py-0.5 rounded">
                              NOT NULL
                            </span>
                          )}
                          {column.default_value && (
                            <span className="bg-gray-100 text-gray-700 text-xs px-1 py-0.5 rounded">
                              DEFAULT: {column.default_value}
                            </span>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}