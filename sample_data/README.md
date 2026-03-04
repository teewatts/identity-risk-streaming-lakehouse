# Snowflake Companion Implementation

This folder contains the Snowflake companion implementation for the Identity Risk Streaming Lakehouse project.

The goal is to complement the Databricks medallion pipeline with a Snowflake-based warehouse implementation focused on staged file ingestion, curated tables, and analytics-ready SQL transformations.

## Scope

This implementation uses a 500K-row sample of the Kaggle Risk-Based Authentication (RBA) dataset and focuses on:

- loading a local CSV file into a Snowflake internal stage
- creating a raw landing table
- creating a cleaned Silver-style table
- creating Gold-style analytics tables
- comparing Snowflake warehouse patterns with the Databricks lakehouse implementation

## Planned Objects

- internal stage for the sample CSV
- CSV file format
- raw landing table
- Silver-style normalized table
- Gold KPI aggregates
- Gold user risk summary

## Notes

This is a companion implementation inside the same project repository. GitHub remains the source of truth for SQL files and documentation.