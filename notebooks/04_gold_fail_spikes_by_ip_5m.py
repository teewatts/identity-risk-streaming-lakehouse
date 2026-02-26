# Databricks Gold Risk Signals - failed login spikes by IP

from pyspark.sql.functions import (
    col,
    window,
    count,
    sum as Fsum,
    when
)

gold_table_name = "gold_fail_spikes_by_ip_5m"

silver_df = spark.table("silver_login_events")

# Aggregate failed login activity by IP over 5-minute windows
gold_fail_spikes_df = (
    silver_df
    .groupBy(
        window(col("event_ts"), "5 minutes"),
        col("ip_address")
    )
    .agg(
        count("*").alias("login_attempts"),
        Fsum(when(col("login_successful") == False, 1).otherwise(0)).alias("failed_logins"),
        Fsum(when(col("login_successful") == True, 1).otherwise(0)).alias("successful_logins"),
        Fsum(when(col("is_attack_ip") == True, 1).otherwise(0)).alias("attack_ip_events"),
        Fsum(when(col("is_account_takeover") == True, 1).otherwise(0)).alias("account_takeover_events")
    )
    .withColumn(
        "failure_rate",
        when(col("login_attempts") > 0, col("failed_logins") / col("login_attempts")).otherwise(None)
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "ip_address",
        "login_attempts",
        "failed_logins",
        "successful_logins",
        "failure_rate",
        "attack_ip_events",
        "account_takeover_events"
    )
)

display(gold_fail_spikes_df)

(
    gold_fail_spikes_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_table_name)
)

print(f"Gold table created: {gold_table_name}")

# Validation query
display(
    spark.table("gold_fail_spikes_by_ip_5m")
    .filter("failed_logins >= 3")
    .orderBy(col("failed_logins").desc(), col("failure_rate").desc())
)