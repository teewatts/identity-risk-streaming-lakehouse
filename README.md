# Identity Risk Streaming Lakehouse

A streaming-first security analytics project built in Databricks using the Kaggle Risk-Based Authentication (RBA) dataset.

This project explores modern data engineering patterns by building a medallion-style lakehouse pipeline for identity-related login events. The goal is to ingest, transform, validate, and analyze login activity in a way that supports risk analytics, operational visibility, and dashboard-ready reporting.

This repository also includes a Snowflake companion implementation to demonstrate equivalent warehouse-oriented ingestion and modeling patterns.

## Project Goals

- Build a Bronze layer for raw login event ingestion
- Build a Silver layer for cleaned, normalized, and validated data
- Build a Gold layer for security and risk analytics
- Create reporting queries and visualizations
- Document architecture, assumptions, and operational considerations

## Current Progress

The initial MVP is complete and includes:

- Bronze ingestion from a Unity Catalog managed volume
- Silver normalization and validation
- Silver quarantine table for invalid records
- Gold KPI aggregates in 5-minute windows
- Gold IP-level failure spike analysis
- Gold user-level risk scoring
- Reporting queries and initial visualizations for:
  - login attempts over time
  - failed logins over time
  - top risky IPs
  - top risky users
  - country-level attack activity

### Companion Snowflake Implementation

A Snowflake companion implementation is included under `snowflake/` and covers:

- internal stage upload and bulk load using `COPY INTO`
- raw landing table
- Silver-style normalized table
- Gold KPI aggregates
- Gold user-level risk scoring

## Architecture Overview

### Bronze

Raw login events are ingested with minimal transformation, including:
- ingestion timestamp
- source file path
- ingestion date

### Silver

Cleaned and standardized login event data with:
- normalized timestamps
- field validation
- standardized field names
- type casting for numeric and boolean values
- quarantine handling for invalid records

### Gold

Analytics-ready tables focused on security and risk insights, including:
- login KPIs
- failed login spikes by IP
- user-level risk scoring
- account takeover indicators
- attack IP activity trends

## Tech Stack

- Databricks
- PySpark
- Delta Lake
- Unity Catalog volumes
- Snowflake (companion implementation)
- SQL
- Structured Streaming concepts
- Kaggle Risk-Based Authentication (RBA) dataset

## Dataset Notes

The source dataset is the Kaggle Risk-Based Authentication (RBA) dataset. Raw source data is not stored in this repository. This repository contains project code, documentation, and setup guidance only.

Development is currently based on a 500K-row sample of the source dataset.

## Quickstart

### Databricks

High-level flow:
1. Upload the sample CSV into a Unity Catalog volume
2. Run notebooks in order:
   - `01_bronze_ingest`
   - `02_silver_normalize`
   - `03_gold_kpis`
   - `04_gold_risk_signals`
   - `05_gold_user_risk`
3. Run the dashboard queries and create visualizations from the Gold tables

### Snowflake

High-level flow:
1. Upload the sample CSV to a named internal stage
2. Run scripts in order from `snowflake/`:
   - `00_setup.sql` through `06_create_gold_user_risk.sql`

## Results Snapshot

On the 500K-row sample:
- 500,000 rows successfully loaded and modeled end to end
- 49,125 events were flagged as attack-IP activity
- 2 events were flagged as account takeover activity

## Current Status

**MVP Complete**

The first version of the Bronze, Silver, and Gold pipeline is working end to end and includes initial reporting outputs in Databricks, plus a companion Snowflake implementation.

## Next Steps

- [ ] Add new device and new browser detection by user
- [ ] Expand data contract and ownership metadata
- [ ] Add additional documentation and operational runbook details
- [ ] Improve dashboard polish and layout
- [ ] Scale to additional input files or larger samples
