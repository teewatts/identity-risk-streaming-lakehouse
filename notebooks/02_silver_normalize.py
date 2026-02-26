# Databricks Silver Normalize - RBA sample

from pyspark.sql.functions import (
    col,
    to_timestamp,
    when,
    trim,
    lower,
    current_timestamp,
    lit
)

silver_table_name = "silver_login_events"
quarantine_table_name = "silver_login_events_quarantine"

bronze_df = spark.table("bronze_login_events")

# Normalize and clean fields
silver_base = (
    bronze_df
    .withColumn("event_ts", to_timestamp(col("login_timestamp")))
    .withColumn("user_id", trim(col("user_id")))
    .withColumn("ip_address", trim(col("ip_address")))
    .withColumn("country", trim(col("Country")))
    .withColumn("region", trim(col("Region")))
    .withColumn("city", trim(col("City")))
    .withColumn("asn", trim(col("ASN")))
    .withColumn("user_agent_string", trim(col("user_agent_string")))
    .withColumn("browser_name_version", trim(col("browser_name_version")))
    .withColumn("os_name_version", trim(col("os_name_version")))
    .withColumn("device_type", trim(col("device_type")))
    .withColumn("rtt_ms", col("rtt_ms").cast("double"))
    .withColumn("login_successful", col("login_successful").cast("boolean"))
    .withColumn("is_attack_ip", col("is_attack_ip").cast("boolean"))
    .withColumn("is_account_takeover", col("is_account_takeover").cast("boolean"))
)

# Validation rules
validated = (
    silver_base
    .withColumn(
        "quarantine_reason",
        when(col("event_ts").isNull(), lit("invalid_timestamp"))
        .when(col("user_id").isNull() | (col("user_id") == ""), lit("missing_user_id"))
        .when(col("ip_address").isNull() | (col("ip_address") == ""), lit("missing_ip_address"))
        .otherwise(lit(None))
    )
)

valid_df = validated.filter(col("quarantine_reason").isNull())
invalid_df = validated.filter(col("quarantine_reason").isNotNull())

# Select clean Silver columns
silver_df = valid_df.select(
    "event_ts",
    "user_id",
    "ip_address",
    "country",
    "region",
    "city",
    "asn",
    "user_agent_string",
    "browser_name_version",
    "os_name_version",
    "device_type",
    "rtt_ms",
    "login_successful",
    "is_attack_ip",
    "is_account_takeover",
    "ingest_ts",
    "source_file",
    "ingest_date"
)

# Select quarantine columns
quarantine_df = invalid_df.select(
    "login_timestamp",
    "user_id",
    "ip_address",
    "country",
    "region",
    "city",
    "asn",
    "user_agent_string",
    "browser_name_version",
    "os_name_version",
    "device_type",
    "rtt_ms",
    "login_successful",
    "is_attack_ip",
    "is_account_takeover",
    "quarantine_reason",
    "ingest_ts",
    "source_file",
    "ingest_date"
)

# Preview
display(silver_df)
display(quarantine_df)

# Save Silver and quarantine tables
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table_name)
)

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(quarantine_table_name)
)

print(f"Silver table created: {silver_table_name}")
print(f"Quarantine table created: {quarantine_table_name}")

#Validation queries
spark.table("silver_login_events").printSchema()
spark.table("silver_login_events_quarantine").printSchema()

display(spark.table("silver_login_events").limit(10))
display(spark.table("silver_login_events_quarantine").limit(10))