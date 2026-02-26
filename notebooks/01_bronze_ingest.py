# Databricks Bronze Ingest - RBA sample

from pyspark.sql.functions import current_timestamp, to_date, col

file_path = "/Volumes/workspace/default/identity_risk_raw/rba_sample_500k.csv"
bronze_table_name = "bronze_login_events"

# Read CSV and include metadata
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(file_path)
    .select("*", "_metadata")
)

# Rename columns to Delta-safe names
renamed_df = (
    df
    .withColumnRenamed("Login Timestamp", "login_timestamp")
    .withColumnRenamed("User ID", "user_id")
    .withColumnRenamed("Round-Trip Time [ms]", "rtt_ms")
    .withColumnRenamed("IP Address", "ip_address")
    .withColumnRenamed("User Agent String", "user_agent_string")
    .withColumnRenamed("Browser Name and Version", "browser_name_version")
    .withColumnRenamed("OS Name and Version", "os_name_version")
    .withColumnRenamed("Device Type", "device_type")
    .withColumnRenamed("Login Successful", "login_successful")
    .withColumnRenamed("Is Attack IP", "is_attack_ip")
    .withColumnRenamed("Is Account Takeover", "is_account_takeover")
)

# Add Bronze metadata columns
bronze_df = (
    renamed_df
    .withColumn("ingest_ts", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingest_date", to_date(col("ingest_ts")))
    .drop("_metadata")
)

display(bronze_df)

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(bronze_table_name)
)

print(f"Bronze table created: {bronze_table_name}")

# Validation query
spark.table("bronze_login_events").printSchema()
display(spark.table("bronze_login_events").limit(10))
