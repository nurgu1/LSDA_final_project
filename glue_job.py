import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, lit

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

BUCKET = "flight-risk-engine-mglgx7" 
try:
    df_schedule = spark.read.option("header", "true").csv(f"s3://{BUCKET}/raw/flights/")
    df_fleet    = spark.read.option("header", "true").csv(f"s3://{BUCKET}/raw/fleet/")
    df_weather  = spark.read.json(f"s3://{BUCKET}/raw/weather/")
    df_solar    = spark.read.json(f"s3://{BUCKET}/raw/solar/")
except Exception as e:
    print(f"Error reading files: {e}")
    sys.exit(1)

step1 = df_schedule.join(df_fleet, "tail_number")

step2 = step1.join(df_weather, step1.destination == df_weather.city)

final_view = step2.join(df_solar, step2.destination == df_solar.city)

current_year = 2025
df_calc = final_view.withColumn("plane_age", current_year - col("manufacture_year").cast("int"))

df_scored = df_calc.withColumn("risk_score", 
    (when(col("plane_age") > 20, 30).otherwise(0)) + 
    (when(col("windspeed") > 15, 40).otherwise(0)) +
    (when(col("weathercode") > 50, 20).otherwise(0))
)

final_df = df_scored.withColumn("prediction",
    when(col("risk_score") >= 50, "HIGH_RISK")
    .when(col("risk_score") >= 20, "MEDIUM_RISK")
    .otherwise("LOW_RISK")
).select(
    "flight_id", "origin", "destination", "tail_number", "plane_age", 
    "temperature", "windspeed", "weathercode", "risk_score", "prediction"
)

final_df.write.mode("overwrite").parquet(f"s3://{BUCKET}/curated/risk_report/")
print("Job Completed Successfully!")
