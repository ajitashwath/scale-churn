"""
Windows compatibility shim:
PySpark's accumulator server expects Unix domain socket classes which are
not available on Windows. For local development on Windows we provide a
lightweight shim that defines `UnixStreamServer` (backed by TCP) so the
import doesn't fail. This is a development workaround — prefer running
inside Docker or WSL for production-like behavior.
"""
import socketserver
if not hasattr(socketserver, "UnixStreamServer"):
    class UnixStreamServer(socketserver.TCPServer):
        pass
    socketserver.UnixStreamServer = UnixStreamServer

import argparse
import logging
import os
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

import sys
try:
    from config import spark_config as cfg
except Exception:
    # When running `python src/etl.py` the package import may fail; add
    # project root to sys.path and retry. This makes the script runnable
    # both as a module and as a standalone script in development.
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from config import spark_config as cfg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl")


def get_spark(app_name: str = "spark-etl") -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for k, v in cfg.SPARK_CONFIGS.items():
        builder = builder.config(k, v)

    master = os.environ.get("SPARK_MASTER", cfg.SPARK_LOCAL)
    builder = builder.master(master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def expected_telco_schema() -> StructType:
    return StructType([
        StructField("customerID", StringType(), nullable=False),
        StructField("gender", StringType(), True),
        StructField("SeniorCitizen", IntegerType(), True),
        StructField("Partner", StringType(), True),
        StructField("Dependents", StringType(), True),
        StructField("tenure", IntegerType(), True),
        StructField("PhoneService", StringType(), True),
        StructField("MultipleLines", StringType(), True),
        StructField("InternetService", StringType(), True),
        StructField("OnlineSecurity", StringType(), True),
        StructField("OnlineBackup", StringType(), True),
        StructField("DeviceProtection", StringType(), True),
        StructField("TechSupport", StringType(), True),
        StructField("StreamingTV", StringType(), True),
        StructField("StreamingMovies", StringType(), True),
        StructField("Contract", StringType(), True),
        StructField("PaperlessBilling", StringType(), True),
        StructField("PaymentMethod", StringType(), True),
        StructField("MonthlyCharges", DoubleType(), True),
        StructField("TotalCharges", DoubleType(), True),
        StructField("Churn", StringType(), True),
    ])


def read_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    logger.info("Reading CSV: %s", path)
    df = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("multiLine", False)
        .schema(schema)
        .csv(path)
    )
    return df


def cleanse_telco(df: DataFrame, numerical: List[str], categorical: List[str]) -> DataFrame:
    string_cols = [c.name for c in df.schema.fields if isinstance(c.dataType, StringType)]
    for c in string_cols:
        df = df.withColumn(c, F.when(F.col(c).isNotNull(), F.trim(F.col(c))).otherwise(None))

    df = df.replace("", None)
    for col in numerical:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(DoubleType()))

    df = df.dropDuplicates()
    if "customerID" in df.columns:
        df = df.filter(F.col("customerID").isNotNull())
    return df


def validate_schema(df: DataFrame, expected: StructType) -> bool:
    actual = {name: str(dtype) for name, dtype in df.dtypes}
    ok = True
    for field in expected.fields:
        if field.name not in actual:
            logger.error("Missing expected column: %s", field.name)
            ok = False
        else:
            if field.dataType.simpleString() not in actual[field.name]:
                logger.warning("Type mismatch for %s: expected %s, actual %s", field.name, field.dataType.simpleString(), actual[field.name])
    if ok:
        logger.info("Schema validation passed (presence check).")
    return ok


def write_parquet(df: DataFrame, output_dir: str, table_name: str, partition_cols: List[str] = None) -> str:
    out_path = os.path.join(output_dir, table_name)
    logger.info("Writing Parquet to %s", out_path)
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(out_path)
    return out_path


def run_etl(input_path: str = None, output_base: str = None, force_csv: bool = True):
    spark = get_spark()
    expected_schema = expected_telco_schema()

    input_path = input_path or cfg.TELCO_CSV
    output_base = output_base or cfg.DATA_PROCESSED
    df = read_csv(spark, input_path, expected_schema)

    raw_count = df.count()
    logger.info("Rows read: %d", raw_count)
    validate_schema(df, expected_schema)

    df_clean = cleanse_telco(df, cfg.NUMERICAL_FEATURES, cfg.CATEGORICAL_FEATURES)
    cleaned_count = df_clean.count()
    logger.info("Rows after cleaning: %d", cleaned_count)

    null_summary = {c: df_clean.filter(F.col(c).isNull()).count() for c in ["customerID", "Churn"] if c in df_clean.columns}
    logger.info("Null counts for key cols: %s", null_summary)

    partition_cols = ["Churn"] if "Churn" in df_clean.columns else None
    out_path = write_parquet(df_clean, output_base, cfg.CLEANED_DATA, partition_cols=partition_cols)

    logger.info("Reading back written Parquet to validate")
    df_back = spark.read.parquet(out_path)
    back_count = df_back.count()
    logger.info("Rows written/read-back: %d", back_count)

    if back_count != cleaned_count:
        logger.error("Row count mismatch after write/read: cleaned=%d back=%d", cleaned_count, back_count)
    else:
        logger.info("Row counts match after write/read.")

    validate_schema(df_back, expected_schema)

    logger.info("Sample rows:")
    df_back.limit(5).show(truncate=False)

    logger.info("Execution plan for final DataFrame (logical plan):")
    logger.info(df_back._jdf.queryExecution().logical().toString())

    spark.stop()


def parse_args():
    parser = argparse.ArgumentParser(description="Distributed ETL for Telco churn dataset using PySpark")
    parser.add_argument("--input", help="Input CSV path", default=None)
    parser.add_argument("--output", help="Base output directory for processed data", default=None)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_etl(input_path=args.input, output_base=args.output)
