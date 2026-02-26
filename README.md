# Identity Risk Streaming Lakehouse

A streaming-first security analytics project built in Databricks using the Kaggle Risk-Based Authentication (RBA) dataset.

This project explores modern data engineering patterns by building a medallion-style lakehouse pipeline for identity-related login events. The goal is to ingest, transform, validate, and analyze login activity in a way that supports risk analytics, operational visibility, and dashboard-ready reporting.

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
- SQL
- Structured Streaming concepts
- Kaggle Risk-Based Authentication (RBA) dataset

## Dataset Notes

The source dataset is the Kaggle Risk-Based Authentication (RBA) dataset. Raw source data is not stored in this repository. This repository contains project code, documentation, and setup guidance only.

Development is currently based on a 500K-row sample of the source dataset.

## Current Status

**MVP Complete**

The first version of the Bronze, Silver, and Gold pipeline is working end to end and includes initial reporting outputs.

## Next Steps

- [ ] Add new device and new browser detection by user
- [ ] Expand data contract and ownership metadata
- [ ] Add additional documentation and operational runbook details
- [ ] Improve dashboard polish and layout
- [ ] Scale to additional input files or larger samples
