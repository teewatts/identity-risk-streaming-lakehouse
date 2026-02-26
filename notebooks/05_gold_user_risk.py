# Databricks Gold User Risk Signals - user-level risk summary

from pyspark.sql.functions import (
    col,
    count,
    sum as Fsum,
    avg,
    when,
    max as Fmax,
    min as Fmin
)

gold_table_name = "gold_risk_signals_by_user"

silver_df = spark.table("silver_login_events")

gold_user_risk_df = (
    silver_df
    .groupBy("user_id")
    .agg(
        count("*").alias("login_attempts"),
        Fsum(when(col("login_successful") == False, 1).otherwise(0)).alias("failed_logins"),
        Fsum(when(col("login_successful") == True, 1).otherwise(0)).alias("successful_logins"),
        Fsum(when(col("is_attack_ip") == True, 1).otherwise(0)).alias("attack_ip_events"),
        Fsum(when(col("is_account_takeover") == True, 1).otherwise(0)).alias("account_takeover_events"),
        avg(col("rtt_ms")).alias("avg_rtt_ms"),
        Fmin("event_ts").alias("first_seen_ts"),
        Fmax("event_ts").alias("last_seen_ts")
    )
    .withColumn(
        "failure_rate",
        when(col("login_attempts") > 0, col("failed_logins") / col("login_attempts")).otherwise(None)
    )
    .withColumn(
        "risk_score",
        (col("failed_logins") * 1.0) +
        (col("attack_ip_events") * 3.0) +
        (col("account_takeover_events") * 5.0)
    )
    .select(
        "user_id",
        "login_attempts",
        "failed_logins",
        "successful_logins",
        "failure_rate",
        "attack_ip_events",
        "account_takeover_events",
        "avg_rtt_ms",
        "risk_score",
        "first_seen_ts",
        "last_seen_ts"
    )
)

display(gold_user_risk_df)

(
    gold_user_risk_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(gold_table_name)
)

print(f"Gold table created: {gold_table_name}")

#validation query
display(
    spark.table("gold_risk_signals_by_user")
    .orderBy(col("risk_score").desc(), col("failed_logins").desc())
)