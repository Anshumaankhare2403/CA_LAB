# Databricks notebook source
# MAGIC %md
# MAGIC # Experiment 04: Azure Databricks — Data Preprocessing & Machine Learning
# MAGIC **Course**: Cloud Analytics (25MCAR0141) | **CO**: CO2
# MAGIC
# MAGIC This notebook demonstrates end-to-end data preprocessing and ML classification
# MAGIC using PySpark MLlib on Azure Databricks with the student performance dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Data from ADLS Gen2

# COMMAND ----------

# Read the student performance dataset from ADLS Gen2 / DBFS
# Update the path below to your mounted storage or DBFS location
df = spark.read.csv(
    "/mnt/rawdata/student_performance.csv",  # or "abfss://rawdata@calablake141.dfs.core.windows.net/student_performance.csv"
    header=True,
    inferSchema=True
)

print(f"Dataset shape: ({df.count()}, {len(df.columns)})")
df.printSchema()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Profiling & Exploratory Analysis

# COMMAND ----------

# Summary statistics
display(df.describe())

# COMMAND ----------

# Null count per column
from pyspark.sql.functions import col, sum as spark_sum, when, count

null_counts = df.select(
    [spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
)
print("=== Null Counts Per Column ===")
display(null_counts)

# COMMAND ----------

# Class distribution (FinalGrade)
print("=== FinalGrade Distribution ===")
display(df.groupBy("FinalGrade").count().orderBy("FinalGrade"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Preprocessing Pipeline

# COMMAND ----------

from pyspark.ml.feature import (
    Imputer, StringIndexer, OneHotEncoder,
    VectorAssembler, StandardScaler
)
from pyspark.ml import Pipeline

# --- 3a. Handle Missing Numeric Values with Imputer ---
numeric_cols = ["Age", "AttendanceRate", "StudyHoursPerWeek"]
imputer = Imputer(
    inputCols=numeric_cols,
    outputCols=[f"{c}_imputed" for c in numeric_cols],
    strategy="median"
)

# --- 3b. Encode Categorical Columns ---
# StringIndexer for categorical features
categorical_cols = ["Gender", "ParentEducation", "LunchType", "TestPrepCourse"]
indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
    for c in categorical_cols
]

# OneHotEncoder for indexed categoricals
encoders = [
    OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec")
    for c in categorical_cols
]

# --- 3c. Index the Target Label ---
label_indexer = StringIndexer(inputCol="FinalGrade", outputCol="label", handleInvalid="keep")

# --- 3d. Assemble Feature Vector ---
feature_cols = (
    ["MathScore", "ReadingScore", "WritingScore"]
    + [f"{c}_imputed" for c in numeric_cols]
    + [f"{c}_vec" for c in categorical_cols]
)
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")

# --- 3e. Normalize Features ---
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=False)

# Build the preprocessing pipeline
preprocessing_pipeline = Pipeline(stages=(
    [imputer] + indexers + encoders + [label_indexer, assembler, scaler]
))

print("Preprocessing pipeline stages:")
for i, stage in enumerate(preprocessing_pipeline.getStages()):
    print(f"  {i+1}. {type(stage).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Train/Test Split & Fit Pipeline

# COMMAND ----------

# Split data 80/20
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"Training set: {train_df.count()} rows")
print(f"Test set:     {test_df.count()} rows")

# Fit preprocessing pipeline on training data
pipeline_model = preprocessing_pipeline.fit(train_df)
train_processed = pipeline_model.transform(train_df)
test_processed = pipeline_model.transform(test_df)

display(train_processed.select("Name", "features", "label"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Train Machine Learning Models

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# --- 5a. Logistic Regression ---
lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=100,
    regParam=0.01,
    elasticNetParam=0.5
)
lr_model = lr.fit(train_processed)
lr_predictions = lr_model.transform(test_processed)

# --- 5b. Random Forest Classifier ---
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=100,
    maxDepth=5,
    seed=42
)
rf_model = rf.fit(train_processed)
rf_predictions = rf_model.transform(test_processed)

print("Models trained successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Model Evaluation

# COMMAND ----------

# Evaluation metrics
evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
evaluator_precision = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
evaluator_recall = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")

def evaluate_model(predictions, model_name):
    metrics = {
        "Model": model_name,
        "Accuracy": round(evaluator_acc.evaluate(predictions), 4),
        "F1 Score": round(evaluator_f1.evaluate(predictions), 4),
        "Precision": round(evaluator_precision.evaluate(predictions), 4),
        "Recall": round(evaluator_recall.evaluate(predictions), 4),
    }
    return metrics

lr_metrics = evaluate_model(lr_predictions, "Logistic Regression")
rf_metrics = evaluate_model(rf_predictions, "Random Forest")

print("=" * 60)
print(f"{'Metric':<20} {'Logistic Regression':<22} {'Random Forest':<22}")
print("=" * 60)
for key in ["Accuracy", "F1 Score", "Precision", "Recall"]:
    print(f"{key:<20} {lr_metrics[key]:<22} {rf_metrics[key]:<22}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Hyperparameter Tuning with CrossValidator

# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Build parameter grid for Random Forest
paramGrid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [50, 100, 200])
    .addGrid(rf.maxDepth, [3, 5, 10])
    .build()
)

# Cross-Validator with 3-fold CV
cv = CrossValidator(
    estimator=rf,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator_acc,
    numFolds=3,
    seed=42
)

cv_model = cv.fit(train_processed)
cv_predictions = cv_model.transform(test_processed)

cv_metrics = evaluate_model(cv_predictions, "RF (Tuned via CV)")
print(f"\nBest Model Accuracy (CrossValidator): {cv_metrics['Accuracy']}")
print(f"Best Model F1 Score: {cv_metrics['F1 Score']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. MLflow Experiment Logging

# COMMAND ----------

import mlflow
import mlflow.spark

mlflow.set_experiment("/Experiments/Exp04_Student_Performance_ML")

with mlflow.start_run(run_name="RF_CrossValidated"):
    # Log parameters
    best_model = cv_model.bestModel
    mlflow.log_param("numTrees", best_model.getNumTrees)
    mlflow.log_param("maxDepth", best_model.getOrDefault("maxDepth"))
    mlflow.log_param("cv_folds", 3)
    mlflow.log_param("dataset_size", df.count())

    # Log metrics
    mlflow.log_metric("accuracy", cv_metrics["Accuracy"])
    mlflow.log_metric("f1_score", cv_metrics["F1 Score"])
    mlflow.log_metric("precision", cv_metrics["Precision"])
    mlflow.log_metric("recall", cv_metrics["Recall"])

    # Log model
    mlflow.spark.log_model(cv_model.bestModel, "random_forest_model")

    print("MLflow run logged successfully!")
    print(f"Run ID: {mlflow.active_run().info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Feature Importance & Confusion Matrix

# COMMAND ----------

import pandas as pd

# Feature importance from best Random Forest model
importances = best_model.featureImportances.toArray()
feature_names = feature_cols
importance_df = pd.DataFrame({
    "Feature": feature_names[:len(importances)],
    "Importance": importances[:len(feature_names)]
}).sort_values("Importance", ascending=False)

print("=== Top Feature Importances ===")
display(spark.createDataFrame(importance_df))

# COMMAND ----------

# Confusion matrix
from pyspark.sql.functions import col

cm = cv_predictions.groupBy("label", "prediction").count()
print("=== Confusion Matrix ===")
display(cm)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Save Processed Data to ADLS Gen2

# COMMAND ----------

# Write the preprocessed and predicted data to output
cv_predictions.select("Name", "MathScore", "ReadingScore", "WritingScore", "label", "prediction") \
    .write.mode("overwrite") \
    .parquet("/mnt/output/student_performance_predictions")

print("Predictions saved to /mnt/output/student_performance_predictions")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Experiment 04 Complete** — Data preprocessing, ML training, hyperparameter tuning,
# MAGIC MLflow tracking, and model evaluation executed successfully on Azure Databricks.
