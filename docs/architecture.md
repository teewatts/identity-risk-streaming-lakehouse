# Architecture Overview

## Project Summary

This project is a streaming-style security analytics pipeline built in Databricks using a sample of the Kaggle Risk-Based Authentication (RBA) dataset.

The goal is to demonstrate a practical medallion architecture for ingesting, validating, transforming, and analyzing identity-related login events for risk analytics and dashboard-ready reporting.

## Data Source

- Source: Kaggle Risk-Based Authentication (RBA) dataset
- Development sample: 500,000-row CSV sample
- File location: Unity Catalog managed volume

## Storage and Ingestion

The source CSV file is uploaded to a Unity Catalog managed volume and read into Databricks for processing.

### Volume Path
`/Volumes/workspace/default/identity_risk_raw/rba_sample_500k.csv`

## Medallion Architecture

### Bronze Layer

Table: `bronze_login_events`

The Bronze layer stores raw ingested records with minimal transformation. It preserves source fidelity and adds ingestion metadata.

#### Bronze responsibilities
- Read raw CSV data from the managed volume
- Preserve source fields
- Add ingestion timestamp
- Add source file path
- Add ingestion date

### Silver Layer

Table: `silver_login_events`  
Quarantine table: `silver_login_events_quarantine`

The Silver layer standardizes and validates the Bronze data so it can be used reliably for downstream analytics.

#### Silver responsibilities
- Normalize timestamp and field types
- Standardize key field names
- Trim and clean selected values
- Cast numeric and boolean fields
- Apply basic validation checks
- Route invalid records to quarantine

## Gold Layer

The Gold layer contains analytics-ready tables designed for reporting and security-focused analysis.

### Gold tables

#### `gold_login_kpis_5m`
5-minute KPI aggregates for:
- login attempts
- failed logins
- successful logins
- success rate
- average RTT
- attack IP events
- account takeover events

#### `gold_fail_spikes_by_ip_5m`
5-minute IP-level risk aggregates for:
- login attempts by IP
- failed login counts
- failure rate
- attack IP activity
- account takeover activity

#### `gold_risk_signals_by_user`
User-level risk summary including:
- total login attempts
- failed logins
- successful logins
- failure rate
- attack IP events
- account takeover events
- average RTT
- first seen / last seen timestamps
- calculated risk score

## Reporting and Visualizations

The project includes visualizations built from Gold and Silver tables, including:

- login attempts over time
- failed logins over time
- top risky IPs
- top risky users
- country-level attack activity

## Current Design Notes

This MVP uses batch processing on a static sample file to simulate a streaming-style architecture. The next phase can extend this design toward incremental ingestion, event-time windows, and more advanced anomaly detection patterns.

## Future Enhancements

- Add new device / new browser detection per user
- Add data contracts and ownership metadata
- Add lineage documentation
- Expand to larger file samples or multiple input files
- Add dashboard polish and operational runbook