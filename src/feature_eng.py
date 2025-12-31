import logging
import os
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

from config import spark_config as cfg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("feature_eng")


def get_spark(app_name: str = "spark-feature-eng") -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for k, v in cfg.SPARK_CONFIGS.items():
        builder = builder.config(k, v)
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
      -- derive average daily charge from MonthlyCharges (approx)
      (MonthlyCharges / 30.0) AS avg_daily_charge,
      -- placeholder for complaints in last 90 days; use column if available
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
