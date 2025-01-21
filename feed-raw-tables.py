# Databricks notebook source
# Load and process diagnosis mapping file
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/healthcare_dlt/default/healthcare_data/diagnosis_mapping.csv")
display(df)

df.write.format("delta").mode("append").saveAsTable("healthcare_dlt.default.raw_diagnosis_map")

# COMMAND ----------

# Load and process patients daily files for 2024
path1 = "/Volumes/healthcare_dlt/default/healthcare_data/patients_daily_file_1_2024.csv"
path2 = "/Volumes/healthcare_dlt/default/healthcare_data/patients_daily_file_2_2024.csv"
path3 = "/Volumes/healthcare_dlt/default/healthcare_data/patients_daily_file_3_2024.csv"

df1 = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{path3}")
df1 = df1.withColumn("admission_date", to_date(col("admission_date")))

display(df1)

df1.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("healthcare_dlt.default.raw_patients_daily")
