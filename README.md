# Identity Risk Streaming Lakehouse

A streaming-first security analytics project built in Databricks using the Kaggle Risk-Based Authentication (RBA) dataset.

This project is designed to explore modern data engineering patterns by building a medallion-style lakehouse pipeline for identity-related login events. The goal is to ingest, transform, validate, and analyze login activity in a way that supports risk analytics, operational visibility, and dashboard-ready reporting.

## Project Goals

- Build a Bronze layer for raw login event ingestion
- Build a Silver layer for cleaned, normalized, and validated data
- Build a Gold layer for security and risk analytics
- Create reporting queries and visualizations
- Document architecture, assumptions, and operational considerations

## Planned Architecture

### Bronze
Raw login events ingested with minimal transformation, including metadata such as ingestion timestamp and source file.

### Silver
Cleaned and standardized login event data with normalized timestamps, validated fields, deduplication logic, and quarantine handling for invalid records.

### Gold
Analytics-ready tables focused on security and risk insights, including login KPIs, failed login spikes, suspicious login patterns, and account takeover indicators.

## Tech Stack

- Databricks
- PySpark
- Delta Lake
- Structured Streaming concepts
- Kaggle Risk-Based Authentication (RBA) dataset

## Dataset Notes

The source dataset is the Kaggle Risk-Based Authentication (RBA) dataset. Raw source data is not stored in this repository. This repository contains project code, documentation, and setup guidance only.

## Current Status

**In Progress**

The initial focus is on building the Bronze, Silver, and Gold pipeline using a 500K-row sample of the source dataset.

## Planned Next Steps

- [ ] Upload sample dataset to Databricks
- [ ] Build Bronze ingestion notebook
- [ ] Build Silver normalization and validation notebook
- [ ] Build Gold risk analytics tables
- [ ] Add dashboard queries and charts
- [ ] Add architecture notes and runbook
