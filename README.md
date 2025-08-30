# NHTSA Data Pipeline Project

This project implements a complete end-to-end data pipeline for processing NHTSA (National Highway Traffic Safety Administration) automotive data using modern data engineering practices with Python, Apache Airflow, and PostgreSQL.

## 🏗️ Project Overview

The project consists of three main tasks:
1. **Task 1**: Python parser for NHTSA data processing (Bronze/Silver layers)
2. **Task 2**: Apache Airflow DAG for automated pipeline orchestration
3. **Task 3**: SQL analytics with Gold layer data marts

## 📊 Complete Data Pipeline Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   SOURCE    │    │   BRONZE    │    │   SILVER    │    │  DATABASE   │    │    GOLD     │
│             │    │             │    │             │    │  (Silver)   │    │ (Analytics) │
│ JSONL.gz    │───▶│ Raw JSON    │───▶│ Cleaned     │───▶│ PostgreSQL  │───▶│ Data Marts  │
│ Files       │    │ Complete    │    │ Filtered    │    │ Tables      │    │ Query       │
│ (~750KB)    │    │ Data        │    │ Deduplicated│    │             │    │ Results     │
│             │    │ (~38MB)     │    │ (~22KB)     │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
      │                    │                    │                    │                    │
   2,820 lines         1,953 records        62 unique           Silver +              Gold
   compressed          all fields           records            Lookup              Analytics
   NHTSA data         preserved            11 fields           Tables              Tables
```

## 🔄 Technical Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           AIRFLOW DAG: nhtsa_data_pipeline                              │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │
│  │   TASK 1        │  │   TASK 2        │  │   TASK 3        │  │   TASK 4        │   │
│  │ parse_load_to   │─▶│ load_to_silver  │─▶│ load_to_database│─▶│ load_to_gold    │   │
│  │ _bronze         │  │                 │  │                 │  │                 │   │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘   │
│           │                     │                     │                     │          │
│           ▼                     ▼                     ▼                     ▼          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │
│  │ Bronze Layer    │  │ Silver Layer    │  │ Silver Tables   │  │ Gold Tables     │   │
│  │ • Parse JSONL.gz│  │ • Extract fields│  │ • processed_    │  │ • gold_top_     │   │
│  │ • Write complete│  │ • Deduplicate   │  │   nhtsa_data    │  │   vehicle_models│   │
│  │   JSON to file  │  │ • Clean data    │  │ • nhtsa_lookup_ │  │ • gold_body_    │   │
│  │ • 1,953 records │  │ • 62 unique VINs│  │   table         │  │   class_dist    │   │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Data Layers Explained:
- **Source**: Raw compressed NHTSA JSONL files from API responses
- **Bronze**: Complete data lake - all raw records preserved for compliance/auditing
- **Silver**: Clean, analytics-ready data with proper data types and deduplication
- **Database**: Structured PostgreSQL tables optimized for queries and joins
- **Gold**: Pre-computed analytical results stored as data marts for fast reporting

## 🐍 Task 1: Python Parser

### What it does

The parser reads compressed NHTSA files and extracts vehicle information fields:

- **Sent_VIN**: 11-character VIN extracted from SearchCriteria
- **Manufacturer_Name**: Car manufacturer name
- **Make**: Car make
- **Model**: Car model  
- **Model_Year**: Year the car was built
- **TRIM**: Car's trim level
- **Vehicle_Type_Id**: Vehicle type identifier
- **Body_Class_Id**: Body class identifier
- **Base_Price**: Base price before taxes and dealer markup
- **NCSA_Make**: NHTSA's make name
- **NCSA_Model**: NHTSA's model name

### How to use

```bash
# Run the parser directly
python src/code/nhtsa_file_parser.py

# Run tests
python test/test_parser.py
```

### Features

- **Duplicate VIN handling**: Automatically skips duplicate VINs
- **Error handling**: Graceful handling of malformed data
- **Multi-stage processing**: Bronze and Silver layer outputs
- **Progress reporting**: Real-time processing status

## 🚀 Task 2: Airflow Pipeline

### Pipeline Tasks (4 Tasks)

#### **Task 1: Parse and Load to Bronze** 🥉
- **Function**: `parse_load_to_bronze()`
- **Purpose**: Parse source NHTSA files and create raw JSON data lake
- **Input**: `data/source/nhtsa_file.jsonl.gz` (750KB compressed)
- **Output**: `data/bronze/complete_nhtsa_data.json` (38MB, 1,953 complete records)
- **Processing Time**: ~5-10 seconds
- **What it does**: 
  - Reads compressed JSONL files line by line
  - Parses all NHTSA API response records
  - Preserves complete data structure (all fields, nested objects)
  - No filtering or transformation - pure data lake approach
  - Validates output file creation and record count

#### **Task 2: Load to Silver** 🥈
- **Function**: `task_load_to_silver()`
- **Purpose**: Process, clean, and filter data for analytics
- **Input**: Source data (re-processed with business logic)
- **Output**: `data/silver/filtered_nhtsa_data.json` (22KB, 62 unique records)
- **Processing Time**: ~3-5 seconds
- **What it does**:
  - Extracts only the 11 required vehicle fields
  - Removes duplicate VINs (deduplication by Sent_VIN)
  - Applies data validation and type conversion
  - Creates analysis-ready, structured dataset
  - Handles missing/null values gracefully

#### **Task 3: Load to Database** 🏗️
- **Function**: `task_load_to_database()`
- **Purpose**: Create and populate PostgreSQL silver tables
- **Input**: Silver layer JSON + Lookup CSV
- **Output**: PostgreSQL tables (`processed_nhtsa_data`, `nhtsa_lookup_table`)
- **Processing Time**: ~10-15 seconds
- **What it does**:
  - Creates database tables with proper schema and indexes
  - Loads lookup table from CSV (82 vehicle type mappings)
  - Inserts silver data with proper data types (INT, FLOAT, VARCHAR)
  - Handles data type conversion and validation
  - Creates performance indexes for fast queries

#### **Task 4: Load to Gold** 🏆
- **Function**: `task_load_to_gold()`
- **Purpose**: Create analytical data marts with pre-computed results
- **Input**: Silver PostgreSQL tables
- **Output**: Gold tables (`gold_top_vehicle_models`, `gold_body_class_distribution`, `gold_vehicle_summary_stats`)
- **Processing Time**: ~5-8 seconds
- **What it does**:
  - Creates Gold layer tables optimized for reporting
  - Executes SQL Query 1: Top 10 most common vehicle models
  - Executes SQL Query 2: Body class distribution analysis
  - Generates summary statistics for dashboards
  - Stores pre-computed results for fast business reporting

### How to run Airflow Pipeline

#### **Option 1: Docker (Recommended for Windows)**
```bash
# Start all services
docker-compose up -d

# Access Airflow UI
# http://localhost:8080 (admin/admin123)
```

#### **Option 2: Manual Installation (Linux/Mac)**
```bash
# Install dependencies
pip install -r requirements_airflow.txt

# Setup Airflow
python airflow_setup.py

# Start services (separate terminals)
airflow webserver --port 8080
airflow scheduler
```

### Access Points
- **Airflow UI**: http://localhost:8080 (admin/admin123)
- **PgAdmin**: http://localhost:8081 (admin@nhtsa.com/admin123)
- **PostgreSQL**: localhost:5432 (nhtsa_user/nhtsa_password)

## 🗄️ Database Schema & Verification

### Silver Layer Tables (Task 3)

#### **`processed_nhtsa_data`** - Main vehicle data
```sql
CREATE TABLE processed_nhtsa_data (
    sent_vin VARCHAR(20),           -- 11-character VIN from SearchCriteria
    manufacturer_name VARCHAR(100), -- Car manufacturer name
    make VARCHAR(50),              -- Vehicle make
    model VARCHAR(100),            -- Vehicle model
    model_year INTEGER,            -- Year the car was built
    trim VARCHAR(100),             -- Car's trim level
    vehicle_type_id INTEGER,       -- Vehicle type identifier
    body_class_id INTEGER,         -- Body class identifier
    base_price FLOAT,              -- Base price before taxes
    ncsa_make VARCHAR(50),         -- NHTSA's make name
    ncsa_model VARCHAR(100),       -- NHTSA's model name
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### **`nhtsa_lookup_table`** - Vehicle type mappings
```sql
CREATE TABLE nhtsa_lookup_table (
    vehicle_type_id INTEGER,       -- NHTSA vehicle type ID
    vehicle_type VARCHAR(100),     -- NHTSA vehicle type name
    body_class_id INTEGER,         -- NHTSA body class ID
    body_class VARCHAR(100),       -- NHTSA body class name
    lx_bodyclass_lvl1 VARCHAR(50), -- LotLinx level 1 classification
    lx_bodyclass_lvl2 VARCHAR(50), -- LotLinx level 2 classification
    incomplete_chassis BOOLEAN     -- Incomplete chassis flag
);
```

### Gold Layer Tables (Task 3 Analytics)

#### **`gold_top_vehicle_models`** - SQL Query 1 Results
```sql
CREATE TABLE gold_top_vehicle_models (
    id SERIAL PRIMARY KEY,
    model_year INTEGER,            -- Vehicle year
    make VARCHAR(50),              -- Vehicle make
    model VARCHAR(100),            -- Vehicle model
    vehicle_count INTEGER,         -- Count of distinct VINs
    rank_position INTEGER,         -- Ranking (1-10)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### **`gold_body_class_distribution`** - SQL Query 2 Results
```sql
CREATE TABLE gold_body_class_distribution (
    id SERIAL PRIMARY KEY,
    lx_bodyclass_lvl1 VARCHAR(50), -- Body class level 1
    bodysegment VARCHAR(50),       -- Body segment (level 2)
    vehicle_count INTEGER,         -- Count of distinct VINs
    percentage_of_total DECIMAL(5,2), -- Percentage of total
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### **`gold_vehicle_summary_stats`** - Summary Metrics
```sql
CREATE TABLE gold_vehicle_summary_stats (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100),      -- Metric name
    metric_value INTEGER,          -- Metric value
    metric_description TEXT,       -- Description
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 📁 Project Structure

```
lotlinx_assignment/
├── src/code/                    # Python source code
│   └── nhtsa_file_parser.py    # Main parser script
├── test/                       # Test files
│   └── test_parser.py          # Parser tests
├── dags/                       # Airflow DAGs
│   └── nhtsa_pipeline_dag.py   # Main pipeline DAG
├── data/                       # Data directories
│   ├── source/                 # Input JSONL files
│   ├── bronze/                 # Raw JSON output
│   └── silver/                 # Processed JSON output
├── docker-compose.yml          # Docker services
├── requirements_airflow.txt    # Python dependencies
└── README.md                   # This file
```

## 🔧 Configuration

### Database Connection (Airflow)
- **Connection ID**: `nhtsa_postgres`
- **Host**: `localhost` (or `postgres` in Docker)
- **Database**: `nhtsa_db`
- **Username**: `nhtsa_user`
- **Password**: `nhtsa_password`
- **Port**: `5432`

## 🔍 Data Verification & Testing

### Method 1: Interactive Database Tool (Recommended)
```bash
# Run the interactive database connection tool
python connect_to_db.py

# Menu options:
# 1. Test connection and show tables
# 2. Run Gold layer DDL  
# 3. Run SQL Query 1 (Top 10 vehicles)
# 4. Run SQL Query 2 (Body class distribution)
# 5. Show Gold layer data
# 6. Custom query
```

### Method 2: Direct SQL Queries
```bash
# Run the SQL test script
python sql_queries/test_queries.py
```

### Method 3: PgAdmin Web Interface
1. **Open PgAdmin**: http://localhost:8081
2. **Login**: admin@nhtsa.com / admin123
3. **Add Server**:
   - Name: NHTSA Database
   - Host: postgres
   - Database: nhtsa_db
   - Username: nhtsa_user
   - Password: nhtsa_password
4. **Browse Tables**: nhtsa_db → Schemas → public → Tables

### Method 4: Command Line PostgreSQL
```bash
# Connect to PostgreSQL container
docker exec -it nhtsa_postgres psql -U nhtsa_user -d nhtsa_db

# Verify data
\dt                                    -- List all tables
SELECT COUNT(*) FROM processed_nhtsa_data;     -- Should show ~62 records
SELECT COUNT(*) FROM nhtsa_lookup_table;       -- Should show 82 records
SELECT COUNT(*) FROM gold_top_vehicle_models;  -- Should show 10 records
```

### Expected Data Verification Results

#### **Silver Layer Verification**
```sql
-- Check processed_nhtsa_data
SELECT COUNT(*) as total_records FROM processed_nhtsa_data;
-- Expected: ~62 records

SELECT COUNT(DISTINCT sent_vin) as unique_vins FROM processed_nhtsa_data;
-- Expected: ~62 unique VINs

SELECT make, COUNT(*) as count FROM processed_nhtsa_data 
GROUP BY make ORDER BY count DESC LIMIT 5;
-- Expected: Top makes like DODGE, FORD, etc.
```

#### **Lookup Table Verification**
```sql
-- Check nhtsa_lookup_table
SELECT COUNT(*) as total_mappings FROM nhtsa_lookup_table;
-- Expected: 82 vehicle type mappings

SELECT DISTINCT lx_bodyclass_lvl1 FROM nhtsa_lookup_table;
-- Expected: PASSENGER CAR, PICKUP, SUV, VAN, MOTORCYCLE, etc.
```

#### **Gold Layer Verification**
```sql
-- SQL Query 1 Results: Top 10 Vehicle Models
SELECT * FROM gold_top_vehicle_models ORDER BY rank_position;
-- Expected: 10 rows with model_year, make, model, vehicle_count, rank_position

-- SQL Query 2 Results: Body Class Distribution  
SELECT * FROM gold_body_class_distribution ORDER BY lx_bodyclass_lvl1;
-- Expected: Multiple rows with body class analysis (excluding motorcycles, buses, convertibles)

-- Summary Statistics
SELECT * FROM gold_vehicle_summary_stats ORDER BY metric_name;
-- Expected: 5 metrics (Total VINs, Makes, Models, Years, Records with Price)
```

### Data Quality Checks
```sql
-- Check for data quality issues
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT sent_vin) as unique_vins,
    COUNT(CASE WHEN sent_vin IS NULL OR sent_vin = '' THEN 1 END) as missing_vins,
    COUNT(CASE WHEN make IS NULL OR make = '' THEN 1 END) as missing_makes,
    COUNT(CASE WHEN model_year IS NOT NULL THEN 1 END) as records_with_year
FROM processed_nhtsa_data;
```

## 📈 Monitoring & Observability

### Airflow Monitoring
- **DAG View**: http://localhost:8080 - Monitor task execution, duration, success/failure
- **Task Logs**: Click on any task to view detailed execution logs
- **Graph View**: Visual representation of task dependencies and status
- **Gantt Chart**: Task execution timeline and performance analysis

### Database Monitoring
- **PgAdmin Dashboard**: http://localhost:8081 - Database performance and query analysis
- **Table Statistics**: Row counts, data types, index usage
- **Query Performance**: Execution plans and optimization recommendations

### Docker Container Monitoring
```bash
# Check container status
docker-compose ps

# View container logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler  
docker-compose logs postgres

# Monitor resource usage
docker stats
```

## 🚨 Troubleshooting

### Common Issues
1. **Windows Airflow Issues**: Use Docker setup instead
2. **Database Connection**: Verify PostgreSQL is running
3. **File Paths**: Ensure data files are in correct directories
4. **Docker Issues**: Check container status with `docker-compose ps`

### Logs
- **Airflow**: Available in UI and `docker-compose logs`
- **Parser**: Console output with progress indicators
- **Database**: Check PgAdmin for data verification

## 📚 Technologies Used

- **Python 3.13**: Core programming language
- **Apache Airflow**: Workflow orchestration
- **PostgreSQL**: Database storage
- **Docker**: Containerization
- **Pandas/JSON**: Data processing
