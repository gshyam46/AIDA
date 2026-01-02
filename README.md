# SQL MVP - Natural Language to SQL Pipeline

A complete Natural Language to SQL (NL→SQL) system that transforms user questions into safe, executable SQL queries against SQLite databases. The system follows a strict pipeline architecture with deterministic transformations, LLM-based semantic parsing, and comprehensive safety validation.

## Features

- **Natural Language Processing**: Ask questions in plain English
- **AI-Powered Analysis**: Uses LLM (OpenAI, Anthropic, etc.) for semantic understanding
- **Safe SQL Generation**: Only generates read-only, parameterized queries
- **Complete Pipeline Transparency**: See every step from question to result
- **SQLite Database Support**: Upload and query your own databases
- **Modern Web Interface**: React/Next.js frontend with real-time results
- **Comprehensive Error Handling**: User-friendly error messages with detailed logging

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   React/Next.js │    │   FastAPI        │    │   SQLite        │
│   Frontend      │◄──►│   Backend        │◄──►│   Database      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                       ┌──────▼──────┐
                       │   Pipeline   │
                       │              │
                       │ 1. Semantic  │
                       │ 2. Normalize │
                       │ 3. Validate  │
                       │ 4. Compile   │
                       │ 5. Execute   │
                       └─────────────┘
```

## Quick Start

### Prerequisites

- Python 3.11+
- Node.js 18+
- Docker (optional)

### Option 1: Docker Compose (Recommended)

1. Clone the repository
2. Set up environment variables:
   ```bash
   cp backend/.env.example backend/.env
   cp frontend/.env.local.example frontend/.env.local
   ```
3. Add your API keys to `backend/.env`:
   ```
   OPENAI_API_KEY=your_openai_api_key_here
   ANTHROPIC_API_KEY=your_anthropic_api_key_here
   ```
4. Start the services:
   ```bash
   docker-compose up --build
   ```
5. Open http://localhost:3000 in your browser

### Option 2: Manual Setup

#### Backend Setup

1. Navigate to the backend directory:
   ```bash
   cd backend
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your API keys
   ```

5. Start the backend server:
   ```bash
   python main.py
   ```

The backend will be available at http://localhost:8000

#### Frontend Setup

1. Navigate to the frontend directory:
   ```bash
   cd frontend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Set up environment variables:
   ```bash
   cp .env.local.example .env.local
   ```

4. Start the development server:
   ```bash
   npm run dev
   ```

The frontend will be available at http://localhost:3000

## Usage

### 1. Upload a Database

- Click "Upload Database" in the sidebar
- Select a SQLite database file (.sqlite or .db)
- The system will automatically extract the schema

### 2. Ask Questions

Try these example questions:
- "What's the total revenue this month?"
- "Count all orders"
- "Average order amount"
- "Show me all customers"

### 3. View Results

The system shows:
- **Final Results**: Your data in a clean table format
- **Semantic Analysis**: How the AI understood your question
- **Business Logic**: How the system mapped your question to database concepts
- **Generated SQL**: The actual SQL query that was executed

## Supported Query Types (V0)

✅ **Supported:**
- Single-table queries
- Aggregations (SUM, COUNT, AVG)
- Time-based filtering ("this month", "last month", "last 7 days")
- Basic filtering with comparison operators

❌ **Not Supported (by design):**
- Table joins
- Subqueries
- Multiple metrics in one query
- Write operations (INSERT, UPDATE, DELETE)
- Complex nested queries

## Configuration

### Business Rules

Edit `backend/config/business_rules.yaml` to customize:
- Metric mappings (e.g., "revenue" → "amount")
- Entity mappings (e.g., "order" → "orders")
- Default filters (e.g., only completed orders)
- Time column mappings

### LLM Providers

The system supports multiple LLM providers through LiteLLM:
- OpenAI (GPT-3.5, GPT-4)
- Anthropic (Claude)
- Azure OpenAI
- And many more

Configure in `backend/config/llm_config.yaml` or via environment variables.

## API Endpoints

### Core Endpoints

- `POST /api/v1/query` - Execute natural language query
- `GET /api/v1/schema` - Get database schema
- `POST /api/v1/upload` - Upload SQLite database
- `GET /api/v1/examples` - Get example queries
- `GET /api/v1/health` - Health check

### Example API Usage

```bash
# Execute a query
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"question": "What is the total revenue this month?"}'

# Get database schema
curl http://localhost:8000/api/v1/schema
```

## Safety Features

- **Read-Only Queries**: Only SELECT statements are allowed
- **SQL Injection Prevention**: All queries use parameterized statements
- **Input Validation**: Comprehensive validation at every pipeline stage
- **Timeout Protection**: Queries are automatically terminated after 30 seconds
- **Schema Validation**: All operations are validated against the actual database schema

## Development

### Project Structure

```
sql-mvp/
├── backend/                 # Python FastAPI backend
│   ├── api/                # API endpoints and models
│   ├── core/               # Core pipeline components
│   ├── config/             # Configuration files
│   └── main.py            # Application entry point
├── frontend/               # Next.js React frontend
│   ├── app/               # Next.js app directory
│   ├── components/        # React components
│   └── lib/               # Utilities and API client
├── database/              # SQLite database files
└── docker-compose.yml     # Docker configuration
```

### Running Tests

```bash
# Backend tests
cd backend
pytest

# Frontend tests (if implemented)
cd frontend
npm test
```

### Adding New Features

1. **Backend**: Add new pipeline components in `backend/core/`
2. **Frontend**: Add new React components in `frontend/components/`
3. **Configuration**: Update business rules in `backend/config/`

## Troubleshooting

### Common Issues

1. **"LLM API key not found"**
   - Make sure you've set `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` in your `.env` file

2. **"Database connection failed"**
   - Ensure your SQLite file is valid and readable
   - Check the file path in the error message

3. **"Query timeout"**
   - Try simplifying your question
   - Add more specific filters to reduce the data size

4. **Frontend can't connect to backend**
   - Make sure the backend is running on port 8000
   - Check the `NEXT_PUBLIC_API_URL` in your frontend `.env.local`

### Logs

- Backend logs: Check the console output where you started the Python server
- Frontend logs: Check the browser console (F12)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions or issues:
1. Check the troubleshooting section above
2. Look at the API documentation at http://localhost:8000/docs
3. Create an issue on GitHub