/* =========================================================
   🎬 NETFLIX DATA ENGINEERING PROJECT
   🥉 PHASE 1: RAW DATA LAYER (BRONZE)
   ========================================================= */

/* =========================================================
   DATABASE SETUP
   ========================================================= */

/*
Objective:
Create a dedicated database for the Netflix project

Why:
Keeps project isolated and organized

CREATE DATABASE netflix_project;
*/


/* =========================================================
   RAW TABLE CREATION
   ========================================================= */

/*
Objective:
Create a raw ingestion table

Important:
- Avoid reserved keywords like "cast"
- Keep all fields as TEXT initially (except year)

Why:
Raw layer should not enforce strict structure
*/

DROP TABLE IF EXISTS netflix_raw;

CREATE TABLE netflix_raw (
    show_id TEXT,
    type TEXT,
    title TEXT,
    director TEXT,
    cast_members TEXT,
    country TEXT,
    date_added TEXT,
    release_year INT,
    rating TEXT,
    duration TEXT,
    listed_in TEXT,
    description TEXT
);


/* =========================================================
   DATA IMPORT
   ========================================================= */

/*
IMPORT DATA

✅ Option A: pgAdmin (Recommended)
Right-click → netflix_raw
Click Import/Export Data

Choose your file: netflix_titles.csv

Settings:
Format: CSV
Header: ✅ YES
Encoding: UTF-8
*/


/* =========================================================
   DATA VALIDATION & INITIAL EXPLORATION
   ========================================================= */

--- VERIFY DATA LOAD

/*
Check if data is loaded successfully
*/
SELECT COUNT(*) FROM netflix_raw;


/*
Preview data
*/
SELECT * FROM netflix_raw LIMIT 5;


/*
Count total rows and check missing values (basic)
*/
SELECT 
    COUNT(*) AS total_rows,
    COUNT(director) AS director_count
FROM netflix_raw;


/*
Check column types (UNDERSTAND DATA STRUCTURE)
*/
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'netflix_raw';


/*
Check unique content types (INITIAL DATA EXPLORATION)
*/
SELECT DISTINCT type
FROM netflix_raw;


/*
Check sample genres
*/
SELECT listed_in
FROM netflix_raw
LIMIT 10;


/*
Check duration format
*/
SELECT DISTINCT duration
FROM netflix_raw
LIMIT 20;


/* =========================================================
   DATA QUALITY ANALYSIS
   ========================================================= */

/*
This query checks data quality by identifying missing values.

Logic:
- COUNT(column) ignores NULL values
- Difference between total rows and column count = missing values

Why important:
Helps us understand how clean or messy the dataset is before analysis
*/
SELECT 
    COUNT(*) AS total_rows,
    COUNT(director) AS director_count,
    COUNT(*) - COUNT(director) AS missing_director
FROM netflix_raw;


/*
STEP 1: Data Quality Check

Objective:
Identify missing values in key columns

Why:
Missing data affects analysis accuracy

Approach:
Using COUNT(column) vs COUNT(*) to detect NULLs

Alternative:
Could also use SUM(CASE WHEN column IS NULL THEN 1 END)
*/


/* =========================================================
   DOCUMENTATION (COMMENTS FOR METADATA)
   ========================================================= */

COMMENT ON TABLE netflix_raw IS 
'Raw Netflix dataset before cleaning';

COMMENT ON COLUMN netflix_raw.duration IS 
'Contains mixed values: minutes for movies and seasons for TV shows';


/* =========================================================
   💡 WHAT WE JUST BUILT
   ========================================================= */

/*
At this point, you now have:

✅ Database created
✅ Raw table created
✅ Real dataset loaded
✅ First exploration done

👉 This is called a Raw Data Layer (Bronze Layer in Data Engineering)
*/


/* =========================================================
   ⚠️ IMPORTANT UNDERSTANDING
   ========================================================= */

/*
Right now your data is:
Messy ❌
Not normalized ❌
Not analysis-ready ❌

*/
