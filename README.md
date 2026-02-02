# Regulatory Records Ingestion Pipeline

Backend ingestion pipeline for regulatory records.

## Requirements

- Node.js >= 16.0.0
- PostgreSQL >= 12 (local or Supabase)
- npm >= 8.0.0

## Quick Start

### 1. Database Setup

**Option A: Local PostgreSQL**
```bash
createdb regulatory_records
psql -d regulatory_records -f schema.sql
```

**Option B: Supabase**
- Create a new project at https://supabase.com
- Run `schema.sql` in the SQL Editor
- Copy connection details from Settings > Database

### 2. Install Dependencies
```bash
npm install
```

### 3. Configure Environment
```bash
cp .env.example .env
# Edit .env with your database credentials
```

### 4. Run Ingestion
```bash
# Bulk ingestion (master dataset)
npm run ingest:bulk

# Recent ingestion (last 72 hours)
npm run ingest:recent

# Run tests
npm test
```

### 5. Start API Server
```bash
npm start
# Server runs on http://localhost:3000
```

## Project Structure

```
regulatory-pipeline/
├── src/
│   ├── connectors/
│   │   └── mock_connector.js   # Data connector (isolated, no business logic)
│   ├── services/
│   │   ├── database.js         # Database connection pool
│   │   ├── ingestion.js        # Ingestion logic with hashing
│   │   ├── alerts.js           # Alert rule matching
│   │   └── export.js           # CSV export with plan gating
│   ├── scripts/
│   │   ├── ingest-bulk.js      # Bulk ingestion runner
│   │   ├── ingest-recent.js    # Recent ingestion runner
│   │   └── test-pipeline.js    # Test suite
│   └── server.js               # API server
├── mock_data/
│   ├── bulk.csv                # Bulk data source (4 records)
│   └── recent.json             # Recent data source (2 records)
├── schema.sql                  # Database schema
├── package.json
├── .env.example
└── README.md
```

## Core Features

### 1. Pluggable Source Connector
- Strict isolation: Connector has NO database access, hashing, or business logic
- Only reads files, parses data, and maps to canonical format
- Two functions: fetchBulk() and fetchRecent(hours)

### 2. Idempotent Ingestion
- Upserts by source_key (never creates duplicates)
- Updates only when content_hash changes
- SHA-256 hash of canonical fields for change detection

### 3. Bulk vs Recent Data Precedence
- Bulk: Master dataset (complete historical data)
- Recent: Fills gaps for last 72 hours until next bulk run
- Recent data defers to bulk when both contain the same record

### 4. Ingestion Run Logging
- Every ingestion attempt logged to ingestion_runs table
- Tracks: source type, timestamps, records fetched/inserted/updated, errors

### 5. Alert Rules
- Filter by entity_name_norm OR region
- Triggers on insert or update when record matches rule
- Logs to alert_logs table

### 6. Subscription Plan Limits
- Starter: 1 alert rule, CSV export blocked
- Pro: 5 alert rules, CSV export allowed
- Team: Unlimited alert rules, CSV export allowed
- Enforced server-side

## API Endpoints

### Ingestion
```
POST /api/ingest/bulk     - Run bulk ingestion
POST /api/ingest/recent   - Run recent ingestion
GET  /api/ingestion/runs  - Get ingestion history
```

### Alerts
```
POST   /api/alerts              - Create alert rule
GET    /api/alerts/user/:userId - Get user's alert rules
DELETE /api/alerts/:alertId     - Delete alert rule
GET    /api/alerts/logs         - Get alert logs
```

### Export
```
GET /api/export/csv    - Export records to CSV (gated by plan)
```

### Data
```
GET /api/records       - Get all records
GET /api/users         - Get all users
GET /health            - Health check
```

## Content Hashing

Content hash is computed from:
- title
- entity_name_raw
- region
- status
- document_url

Records are updated only when the hash changes.

## Testing

```bash
npm test
```

Tests verify:
- Bulk ingestion (4 records inserted)
- Recent ingestion (1 new record, 1 skipped duplicate)
- Content hash change detection
- No duplicate records created
- Ingestion run logging
- Alert rule creation with plan limits
- Alert triggering on matching records
- CSV export plan gating

## License

MIT