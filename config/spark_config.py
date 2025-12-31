SPARK_LOCAL = "local[*]"
SPARK_CLUSTER = "spark://spark-master:7077"

DRIVER_MEMORY = "4g"
EXECUTOR_MEMORY = "2g"

DATA_RAW = "data/raw"
DATA_PROCESSED = "data/processed"
DATA_MODELS = "data/models"

TELCO_CSV = "data/WA_Fn-UseC_-Telco-Customer-Churn.csv"

CLEANED_DATA = "telco_cleaned"
ML_PIPELINE = "churn_prediction_pipeline"
PREDICTIONS = "predictions"

RF_NUM_TREES = 50
RF_MAX_DEPTH = 10
RF_SEED = 42

TRAIN_RATIO = 0.8
TEST_RATIO = 0.2

NUMERICAL_FEATURES = [
    "SeniorCitizen",
    "tenure",
    "MonthlyCharges",
    "TotalCharges"
]

CATEGORICAL_FEATURES = [
    "gender",
    "Partner",
    "Dependents",
    "PhoneService",
    "MultipleLines",
    "InternetService",
    "OnlineSecurity",
    "OnlineBackup",
    "DeviceProtection",
    "TechSupport",
    "StreamingTV",
    "StreamingMovies",
    "Contract",
    "PaperlessBilling",
    "PaymentMethod"
]

SPARK_CONFIGS = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.driver.memory": DRIVER_MEMORY,
    "spark.executor.memory": EXECUTOR_MEMORY
}