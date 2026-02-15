import socketserver
if not hasattr(socketserver, "UnixStreamServer"):
    class UnixStreamServer(socketserver.TCPServer):
        pass
    socketserver.UnixStreamServer = UnixStreamServer

import logging
import os
import sys
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler,
    StringIndexer,
    OneHotEncoder,
)
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

try:
    from config import spark_config as cfg
except Exception:
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))
    from config import spark_config as cfg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("feature_eng")


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
    
    logger.debug("Searching for Java in %d locations", len(search_paths))
    for base_path in search_paths:
        logger.debug("Checking path: %s", base_path)
        if not os.path.exists(base_path):
            logger.debug("Path does not exist: %s", base_path)
            continue
        
        java_exe = os.path.join(base_path, "bin", "java.exe")
        logger.debug("Checking for java.exe at: %s", java_exe)
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


def get_spark(app_name: str = "spark-feature-eng") -> SparkSession:
    setup_java_home()
    
    builder = SparkSession.builder.appName(app_name)
    for k, v in cfg.SPARK_CONFIGS.items():
        builder = builder.config(k, v)
    
    if os.name == 'nt':
        if not os.environ.get("HADOOP_HOME") and not os.environ.get("HADOOP_HOME_DIR"):
            dummy_hadoop_home = os.path.join(os.path.expanduser("~"), ".hadoop")
            bin_dir = os.path.join(dummy_hadoop_home, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            
            winutils_path = os.path.join(bin_dir, "winutils.exe")
            if not os.path.exists(winutils_path):
                try:
                    import urllib.request
                    winutils_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.0/bin/winutils.exe"
                    logger.info("Downloading winutils.exe for Windows Hadoop support...")
                    urllib.request.urlretrieve(winutils_url, winutils_path)
                    logger.info("winutils.exe downloaded successfully")
                except Exception as e:
                    logger.warning(f"Could not download winutils.exe: {e}. Spark may have issues on Windows.")
            
            os.environ["HADOOP_HOME"] = dummy_hadoop_home
            builder = builder.config("spark.hadoop.hadoop.home.dir", dummy_hadoop_home)

        # On Windows, Hadoop native libraries are usually missing, which causes
        # UnsatisfiedLinkError in NativeIO$Windows.access0 when listing files.
        # Tell Spark/Hadoop to skip native libs and fall back to pure-Java code.
        builder = builder.config("spark.hadoop.io.native.lib.available", "false")
    
    master = os.environ.get("SPARK_MASTER", cfg.SPARK_LOCAL)
    spark = builder.master(master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_cleaned(spark: SparkSession, base_path: str = None) -> DataFrame:
    base_path = base_path or cfg.DATA_PROCESSED
    path = os.path.join(base_path, cfg.CLEANED_DATA)
    logger.info("Loading cleaned Parquet from %s", path)
    return spark.read.parquet(path)


def compute_aggregates(df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("telco")

    sql = """
    SELECT
      customerID,
      (MonthlyCharges / 30.0) AS avg_daily_charge,
      COALESCE(NumComplaints90Days, 0) AS complaints_90d,
      Churn,
      MonthlyCharges,
      TotalCharges,
      tenure
    FROM telco
    """
    agg = df.sparkSession.sql(sql)
    agg = agg.withColumn("avg_daily_charge", F.col("avg_daily_charge").cast(DoubleType()))
    return agg

def assemble_features(df: DataFrame, numerical: List[str], categorical: List[str]) -> tuple[DataFrame, object]:
    if "Churn" in df.columns:
        label_indexer = StringIndexer(inputCol="Churn", outputCol="label", handleInvalid="keep")
    else:
        raise ValueError("Churn column required for supervised training")

    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in categorical if c in df.columns]
    encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_enc") for c in categorical if c in df.columns]
    feature_cols = []
    for n in numerical:
        if n in df.columns:
            feature_cols.append(n)

    if "avg_daily_charge" in df.columns:
        feature_cols.append("avg_daily_charge")
    if "complaints_90d" in df.columns:
        feature_cols.append("complaints_90d")

    enc_cols = [f"{c}_enc" for c in categorical if c in df.columns]
    feature_cols.extend(enc_cols)

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")

    stages = [label_indexer] + indexers + encoders + [assembler]
    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(df)
    df_transformed = model.transform(df)
    return df_transformed, model


def train_random_forest(df: DataFrame):
    train_ratio = cfg.TRAIN_RATIO
    test_ratio = cfg.TEST_RATIO
    train, test = df.randomSplit([train_ratio, test_ratio], seed=cfg.RF_SEED)
    logger.info("Train records: %d, Test records: %d", train.count(), test.count())

    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=cfg.RF_NUM_TREES, maxDepth=cfg.RF_MAX_DEPTH, seed=cfg.RF_SEED)
    pipeline = Pipeline(stages=[rf])
    model = pipeline.fit(train)

    preds = model.transform(test)
    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = evaluator.evaluate(preds)
    logger.info("Test AUC: %.4f", auc)

    return model, auc


def check_for_shuffles(df: DataFrame) -> bool:
    plan = df._jdf.queryExecution().executedPlan().toString()
    has_shuffle = "Exchange" in plan or "ShuffledHashJoin" in plan
    if has_shuffle:
        logger.info("Detected possible shuffle in execution plan.")
    else:
        logger.info("No obvious shuffle operators detected in plan.")
    return has_shuffle


def persist_pipeline(models_base: str, preproc_model, rf_model):
    base = models_base or cfg.DATA_MODELS
    os.makedirs(base, exist_ok=True)
    preproc_path = os.path.join(base, f"{cfg.ML_PIPELINE}_preproc")
    rf_path = os.path.join(base, f"{cfg.ML_PIPELINE}_rf")

    logger.info("Saving preprocessing pipeline to %s", preproc_path)
    preproc_model.write().overwrite().save(preproc_path)

    logger.info("Saving RF model pipeline to %s", rf_path)
    rf_model.write().overwrite().save(rf_path)

    return preproc_path, rf_path


def run_feature_engineering(input_path: str = None, models_path: str = None):
    spark = get_spark()
    df = load_cleaned(spark, input_path)

    agg = compute_aggregates(df)
    logger.info("Aggregate logical plan:")
    agg.explain(True)

    transformed_df, preproc_model = assemble_features(agg, cfg.NUMERICAL_FEATURES, cfg.CATEGORICAL_FEATURES)
    check_for_shuffles(transformed_df)

    rf_model, auc = train_random_forest(transformed_df)
    preproc_path, rf_path = persist_pipeline(models_path or cfg.DATA_MODELS, preproc_model, rf_model)
    spark.stop()
    logger.info("Feature engineering + training complete. AUC=%.4f", auc)
    logger.info("Saved preproc: %s, rf: %s", preproc_path, rf_path)

if __name__ == "__main__":
    run_feature_engineering()
