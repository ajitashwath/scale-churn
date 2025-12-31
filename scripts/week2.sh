#!/bin/bash

echo "Spark Scale Churn - Week 2"
echo ""

if [ ! -d "data/processed/telco_cleaned" ]; then
    echo "Processed data not found. Please run week1.sh to complete the ETL pipeline first."
    exit 1
fi

echo "Starting distributed model training and evaluation pipeline"
echo ""

python src/feature_eng.py

if [ $? -eq 0 ]; then
    echo "Feature engineering completed successfully."
else
    echo "Feature engineering failed."
    exit 1
fi