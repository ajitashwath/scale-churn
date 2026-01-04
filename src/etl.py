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
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from config import spark_config as cfg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl")


def setup_java_home():
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        java_exe = os.path.join(java_home, "bin", "java.exe")
        if os.path.exists(java_exe):
            logger.info("JAVA_HOME already set to: %s", java_home)
            return
    
    search_paths = []
    if os.name == 'nt':
        local_appdata = os.environ.get("LOCALAPPDATA", "")
        if local_appdata:
            search_paths.append(os.path.join(local_appdata, "Java", "jdk-17"))
        
        program_files = os.environ.get("ProgramFiles", "")
        program_files_x86 = os.environ.get("ProgramFiles(x86)", "")
        
        if program_files:
            search_paths.extend([
                os.path.join(program_files, "Java"),
                os.path.join(program_files, "Eclipse Adoptium"),
            ])
        if program_files_x86:
            search_paths.append(os.path.join(program_files_x86, "Java"))
        
        if local_appdata:
            search_paths.append(os.path.join(local_appdata, "Programs", "Java"))
    
    for base_path in search_paths:
        if not os.path.exists(base_path):
            continue
        
        java_exe = os.path.join(base_path, "bin", "java.exe")
        if os.path.exists(java_exe):
            os.environ["JAVA_HOME"] = base_path
            logger.info("Found Java and set JAVA_HOME to: %s", base_path)
            return
        
        try:
            for item in os.listdir(base_path):
                jdk_path = os.path.join(base_path, item)
                if os.path.isdir(jdk_path):
                    java_exe = os.path.join(jdk_path, "bin", "java.exe")
                    if os.path.exists(java_exe):
                        os.environ["JAVA_HOME"] = jdk_path
                        logger.info("Found Java and set JAVA_HOME to: %s", jdk_path)
                        return
        except (OSError, PermissionError):
            continue
    
    logger.error("Java not found! Please install Java and set JAVA_HOME.")
    logger.error("You can run: .\\setup_java.ps1")
    raise RuntimeError(
        "JAVA_HOME is not set and Java could not be found automatically. "
        "Please install Java (JDK 8 or later) and set JAVA_HOME, or run setup_java.ps1"
    )


def get_spark(app_name: str = "spark-etl") -> SparkSession:
    setup_java_home()
    
    builder = SparkSession.builder.appName(app_name)
    for k, v in cfg.SPARK_CONFIGS.items():
        builder = builder.config(k, v)

    if os.name == 'nt':
        if not os.environ.get("HADOOP_HOME") and not os.environ.get("HADOOP_HOME_DIR"):
            dummy_hadoop_home = os.path.join(os.path.expanduser("~"), ".hadoop")
            bin_dir = os.path.join(dummy_hadoop_home, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            
            os.environ["HADOOP_HOME"] = dummy_hadoop_home
            builder = builder.config("spark.hadoop.hadoop.home.dir", dummy_hadoop_home)
        
        os.environ["HADOOP_OPTS"] = "-Djava.library.path="
        builder = builder.config("spark.hadoop.io.native.lib.available", "false")
        builder = builder.config("spark.hadoop.fs.file.impl.disable.cache", "true")
        builder = builder.config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.ManagedCommitProtocol")

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
    if partition_cols and os.name != 'nt':
        writer = writer.partitionBy(*partition_cols)
    elif partition_cols and os.name == 'nt':
        logger.warning("Partitioning disabled on Windows to avoid native IO issues")
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_etl(input_path=args.input, output_base=args.output)
