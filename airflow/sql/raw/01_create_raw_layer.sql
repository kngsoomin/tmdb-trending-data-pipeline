-- This script stores original TMDB payload
-- It defines file format for file-based ingestion

-- Create database if it does not exist
CREATE DATABASE IF NOT EXISTS DEV;

-- Create schemas for the different layers
CREATE SCHEMA IF NOT EXISTS DEV.RAW;
CREATE SCHEMA IF NOT EXISTS DEV.STG;
CREATE SCHEMA IF NOT EXISTS DEV.DW;

-- RAW file format for TMDB JSON
CREATE OR REPLACE FILE FORMAT DEV.RAW.TMDB_JSON_FF
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    IGNORE_UTF8_ERRORS = TRUE;

-- Stage for TMDB trending files
CREATE OR REPLACE STAGE DEV.RAW.TMDB_TRENDING_STAGE
    FILE_FORMAT = DEV.RAW.TMDB_JSON_FF;

-- RAW table to store original TMDB payload
CREATE TABLE IF NOT EXISTS DEV.RAW.TMDB_TRENDING_RAW (
    LOAD_DATE       DATE            DEFAULT CURRENT_DATE,
    LOAD_TS         TIMESTAMP_LTZ   DEFAULT CURRENT_TIMESTAMP,
    SOURCE          STRING          DEFAULT 'tmdb_trending',
    MEDIA_TYPE      STRING, -- 'all', 'movie', 'tv', etc.
    TIME_WINDOW     STRING, -- 'day' or 'week'
    PAYLOAD         VARIANT -- full JSON payload from api  
);
