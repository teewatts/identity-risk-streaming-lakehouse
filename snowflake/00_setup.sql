-- 00_setup.sql
-- Purpose: create core Snowflake objects for this project

-- Optional: pick a warehouse you already have access to
-- Replace COMPUTE_WH if needed
USE WAREHOUSE COMPUTE_WH;

-- Create a project database
CREATE DATABASE IF NOT EXISTS IDENTITY_RISK_DB;

-- Create a schema for this project
CREATE SCHEMA IF NOT EXISTS IDENTITY_RISK_DB.RAW_LAKEHOUSE;

-- Set context
USE DATABASE IDENTITY_RISK_DB;
USE SCHEMA RAW_LAKEHOUSE;
