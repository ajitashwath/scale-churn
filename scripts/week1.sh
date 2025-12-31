#!/bin/bash

echo "Spark Scale Churn - Week 1"

if [ ! -f "data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv" ]; then
    echo "Downloading Telco Customer Churn dataset..."
    mkdir -p data/raw
    wget -O data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv https://raw.githubusercontent.com/blastchar/telco-churn/master/WA_Fn-UseC_-Telco-Customer-Churn.csv
else
    echo "Dataset already exists. Skipping download."
fi

echo "Dataset is ready."
echo "Starting distributed ETL pipeline"

python src/etl.py

if [ $? -eq 0]; then
    echo "ETL pipeline completed successfully."
else
    echo "ETL pipeline failed."
    exit 1
fi