# Spark Scale Churn
A Apache Spark pipeline for customer churn prediction using machine learning.

## Overview

This project implements an end-to-end MLOps pipeline for analyzing telecom customer churn:
1. **ETL Pipeline** (`src/etl.py`) - Ingests, validates, cleanses, and transforms raw customer data
2. **Feature Engineering** (`src/feature_eng.py`) - Creates ML features, trains Random Forest classifier, evaluates performance
3. **Distributed Processing** - Runs locally or on Spark clusters via Docker Compose

**Key Features:**
- Automated Java/Hadoop environment setup (Windows, Linux, macOS)
- Production-ready data validation with schema enforcement
- Spark SQL optimizations (adaptive query execution, partition coalescing)
- Model persistence for deployment
- Docker Compose for multi-node Spark clusters
- Comprehensive validation and testing scripts

## Prerequisites
- **Python 3.8+** with pip
- **Java 8+** (JDK 11 or 17 recommended)
- **Docker & Docker Compose** (optional, for cluster mode)

### Quick Java Setup (Windows)

If you don't have Java installed:

```powershell
.\scripts\setup_java.ps1
```

This will download and configure OpenJDK 17 automatically.

## Installation

### 1. Clone and Setup

```bash
git clone <repository-url>
cd spark-scale-churn
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Validate Setup

```powershell
.\scripts\test_setup.ps1
```

This checks Python, Java, PySpark, data files, and Spark session creation.

## Usage

### Local Execution (Single Machine)

**Run Complete Pipeline:**

```powershell
.\scripts\run_pipeline.ps1
```

This executes:
1. ETL: Cleanses data → `data/processed/telco_cleaned/`
2. Feature Engineering: Trains model → `data/models/`

**Run Individual Steps:**

```bash
# ETL only
python src/etl.py --input data/WA_Fn-UseC_-Telco-Customer-Churn.csv --output data/processed

# Feature engineering only (requires ETL output)
python src/feature_eng.py
```

### Distributed Execution (Docker Cluster)

**Start Spark Cluster:**

```bash
docker-compose up -d
```

This creates:
- 1 Spark Master (Web UI: http://localhost:8080)
- 2 Spark Workers (4GB RAM, 4 cores total)
- Jupyter Notebook (http://localhost:8888)

**Run Pipeline on Cluster:**

```bash
# Connect to master container
docker exec -it spark-master bash

# Inside container
cd /opt/bitnami/spark/app
spark-submit --master spark://spark-master:7077 etl.py
spark-submit --master spark://spark-master:7077 feature_eng.py
```

**Stop Cluster:**

```bash
docker-compose down
```

## Project Structure

```
spark-scale-churn/
├── config/
│   └── spark_config.py          # Spark configs, feature definitions
├── data/
│   ├── WA_Fn-UseC_-Telco-Customer-Churn.csv  # Input data
│   ├── processed/               # ETL outputs (Parquet)
│   └── models/                  # Trained ML models
├── scripts/
│   ├── test_setup.ps1          # Environment validation
│   ├── run_pipeline.ps1        # Pipeline runner
│   └── setup_java.ps1          # Java auto-installer (Windows)
├── src/
│   ├── etl.py                  # ETL pipeline
│   ├── feature_eng.py          # Feature engineering & training
│   └── utils.py                # Shared utilities
├── docker-compose.yml          # Multi-node Spark cluster
├── Dockerfile                  # Spark container image
└── requirements.txt            # Python dependencies
```

## Pipeline Details

### ETL (`etl.py`)

**Input:** CSV file with telecom customer data  
**Output:** Partitioned Parquet files (by Churn status)

**Steps:**
1. Schema validation against expected structure
2. Data cleansing (null handling, trimming, type casting)
3. Deduplication
4. Parquet writing with optional partitioning

**Optimizations:**
- Adaptive query execution enabled
- Partition coalescing for small files
- Kryo serialization for speed

### Feature Engineering (`feature_eng.py`)

**Input:** Cleaned Parquet from ETL  
**Output:** Trained Random Forest pipeline

**Steps:**
1. Compute aggregates (daily charges, complaints)
2. String indexing + one-hot encoding for categorical features
3. Vector assembly for ML input
4. Train/test split (80/20)
5. Random Forest training (50 trees, depth 10)
6. AUC evaluation on test set
7. Model persistence to disk

**Features Used:**
- Numerical: `tenure`, `MonthlyCharges`, `TotalCharges`, `SeniorCitizen`
- Categorical: `Contract`, `InternetService`, `PaymentMethod`, etc.
- Derived: `avg_daily_charge`, `complaints_90d` (if available)

## Configuration

Edit `config/spark_config.py` to customize:

```python
# Spark resources
DRIVER_MEMORY = "4g"
EXECUTOR_MEMORY = "2g"

# Model hyperparameters
RF_NUM_TREES = 50
RF_MAX_DEPTH = 10

# Train/test split
TRAIN_RATIO = 0.8
TEST_RATIO = 0.2
```

## Troubleshooting

### Windows: `FileNotFoundException: Could not locate winutils.exe`

**Solution:** The code auto-downloads `winutils.exe`. If it fails:

```powershell
.\scripts\download_winutils.ps1
```

### `JAVA_HOME is not set`

**Solution:** Install Java and set `JAVA_HOME`:

```powershell
# Auto-install (Windows)
.\scripts\setup_java.ps1

# Manual
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
```

### `ModuleNotFoundError: No module named 'pyspark'`

**Solution:**

```bash
pip install -r requirements.txt
```

### Docker: `cannot connect to Docker daemon`

**Solution:** Start Docker Desktop, then retry `docker-compose up`.

## Model Performance

After running `feature_eng.py`, check logs for:

```
Test AUC: 0.XXXX
```

AUC > 0.75 indicates reasonable churn prediction performance.

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request