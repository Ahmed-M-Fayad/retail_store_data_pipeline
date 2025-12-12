# Retail Data Pipeline

Automated ETL pipeline for retail data: quality checks → transformations → SQL Server → analytics reports.

## 🎯 Overview

Intelligent data pipeline that automatically analyzes data quality, cleans and transforms data, loads into SQL Server, and generates business intelligence reports.

**Key Features:**
- Automated data quality assessment with scoring
- Smart configuration-driven transformations
- SQL Server integration with proper relationships
- Pre-built analytics queries and visualizations

## 🏗️ Architecture

```
Raw CSV → Quality Check → Transform → SQL Server → Reports
          (config.yaml)   (cleaned)   (RetailDB)   (HTML/CSV)
```

**Tech Stack:** Python, Pandas, SQLAlchemy, PyODBC, Matplotlib

---

## 📁 Project Structure

```
retail-data-pipeline/
├── scripts/           # Main scripts (main.py, reporting_script.py)
├── src/
│   ├── extract/      # Data quality check
│   ├── transform/    # Data cleaning & transformation
│   ├── load/         # SQL Server loader
│   └── utils/        # Config utilities
├── config/           # pipeline_config.yaml
├── data/
│   ├── raw/          # Source CSVs (9 files)
│   └── processed/    # Cleaned CSVs
├── tests/            # Unit tests
└── reports/          # Generated reports (CSV/HTML/charts)
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.7+
- SQL Server (2016+)
- ODBC Driver 17 for SQL Server

### Installation

```bash
# Clone and setup
git clone https://github.com/yourusername/retail-data-pipeline.git
cd retail-data-pipeline
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Create directories and add CSV files
mkdir -p data/raw data/processed config reports
# Place your 9 CSV files in data/raw/
```

### Configuration

**1. SQL Server** (`src/load/sql_loader.py`):
```python
SQL_SERVER = 'YOUR_SERVER_NAME'  # e.g., 'localhost'
SQL_DATABASE = 'RetailDB'
USE_WINDOWS_AUTH = True  # or False for SQL auth
```

**2. Reporting** (`scripts/reporting_script.py`): Same as above

**3. Pipeline Options** (`scripts/main.py`):
```python
SKIP_CHECK = False      # Skip quality check
SKIP_TRANSFORM = False  # Skip transformation  
SKIP_LOAD = False       # Skip SQL loading
AUTO_CONTINUE = True    # Auto-proceed between steps
```

---

## 💻 Usage

### Complete Pipeline
```bash
cd scripts
python main.py  # Runs: Check → Transform → Load
```

### Individual Stages
```bash
# Quality check only
python src/extract/data_quality_check.py

# Transform only  
python src/transform/transform_pipeline.py

# Load to SQL only
python src/load/sql_loader.py

# Generate reports
python scripts/reporting_script.py
```

## 🔄 Pipeline Stages

### 1. Quality Check
Analyzes data and generates `pipeline_config.yaml`:
- Detects duplicates, missing values, type issues
- Calculates quality score
- Determines required transformations

### 2. Transform
Cleans and transforms based on config:
- Standardizes columns (lowercase, underscores)
- Removes duplicates, handles nulls
- Converts data types
- Enriches products, calculates totals

### 3. Load
Creates schema and loads to SQL Server:
- 9 tables with proper foreign keys
- Load order respects dependencies
- Verifies row counts

### 4. Analytics
Generates reports from SQL Server:
- 15+ business queries (sales, inventory, staff, customers)
- 9 visualization types
- HTML dashboard + CSV exports

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Specific module
pytest tests/test_transform.py -v

# With coverage
pytest tests/ --cov=src
```

---

## 🔧 Troubleshooting

| Issue | Solution |
|-------|----------|
| **SQL Connection Failed** | Verify server running, check server name, ensure firewall allows port 1433 |
| **ODBC Driver Missing** | Install [ODBC Driver 17](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) |
| **Module Import Error** | Run from correct directory, activate virtual environment |
| **Date Parsing Issues** | Pipeline auto-corrects malformed years (1016→2016) |
| **CSV Files Not Found** | Place all 9 CSVs in `data/raw/` directory |

**Test SQL Connection:**
```python
import pyodbc
print(pyodbc.drivers())  # Should show ODBC Driver 17
```

## 📄 License

MIT License - Copyright (c) 2025 Ahmed Mohammad Abdul Badi Fayad

---

**Built for retail data analytics** | *Last Updated: December 2025*
