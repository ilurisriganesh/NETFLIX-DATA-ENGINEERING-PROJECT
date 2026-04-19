/*
Objective:
Create a dedicated database for the Netflix project

Why:
Keeps project isolated and organized

CREATE DATABASE netflix_project;
*/

/*
Objective:
Create a raw ingestion table

Important:
- Avoid reserved keywords like "cast"
- Keep all fields as TEXT initially (except year)

Why:
Raw layer should not enforce strict structure
*/

DROP TABLE netflix_raw;

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

/* IMPORT DATA
✅ Option A: pgAdmin (Recommended)
Right-click → netflix_raw
Click Import/Export Data
Choose your file: netflix_titles.csv
Settings:
Format: CSV
Header: ✅ YES
Encoding: UTF-8
*/
---VERIFY DATA LOAD
/*
Check if data is loaded successfully
*/

SELECT COUNT(*) FROM netflix_raw;
--and
/*
Preview data
*/
SELECT * FROM netflix_raw LIMIT 5;

-- Count total rows and check missing values
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
Check unique content types(INITIAL DATA EXPLORATION)
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

/* 💡 WHAT YOU JUST BUILT

At this point, you now have:

✅ Database created
✅ Raw table created
✅ Real dataset loaded
✅ First exploration done

👉 This is called a Raw Data Layer (Bronze Layer in Data Engineering)*/

/* ⚠️ IMPORTANT UNDERSTANDING

Right now your data is:
Messy ❌
Not normalized ❌
Not analysis-ready ❌
👉 Perfect. This is how real-world data looks.*/

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

COMMENT ON TABLE netflix_raw IS 'Raw Netflix dataset before cleaning';

COMMENT ON COLUMN netflix_raw.duration IS 'Contains mixed values: minutes for movies and seasons for TV shows';

/* 🚀 PHASE 2: DATA CLEANING (COMPLETE SYSTEM SCRIPT)*/

/* =========================================================
   PHASE 2: DATA CLEANING & PREPARATION
   Objective: Transform raw Netflix data into clean dataset
   ========================================================= */


/* =========================================================
   STEP 1: DATA QUALITY AUDIT
   ========================================================= */

SELECT 
    COUNT(*) AS total_rows,

    COUNT(director) AS director_count,
    COUNT(cast_members) AS cast_count,
    COUNT(country) AS country_count,
    COUNT(date_added) AS date_added_count,

    COUNT(*) - COUNT(director) AS missing_director,
    COUNT(*) - COUNT(cast_members) AS missing_cast,
    COUNT(*) - COUNT(country) AS missing_country,
    COUNT(*) - COUNT(date_added) AS missing_date

FROM netflix_raw;


/* =========================================================
   STEP 2: CLEAN DATE COLUMN
   Problem: Stored as TEXT
   Solution: Convert to DATE
   ========================================================= */

ALTER TABLE netflix_raw
ADD COLUMN date_added_clean DATE;

UPDATE netflix_raw
SET date_added_clean = TO_DATE(date_added, 'Month DD, YYYY');


/* =========================================================
   STEP 3: FEATURE ENGINEERING (DATE)
   ========================================================= */

ALTER TABLE netflix_raw
ADD COLUMN year_added INT,
ADD COLUMN month_added INT;

UPDATE netflix_raw
SET 
    year_added = EXTRACT(YEAR FROM date_added_clean),
    month_added = EXTRACT(MONTH FROM date_added_clean);


/* =========================================================
   STEP 4: CLEAN DURATION COLUMN
   ========================================================= */

ALTER TABLE netflix_raw
ADD COLUMN duration_minutes INT,
ADD COLUMN seasons INT;

/* Movies */
UPDATE netflix_raw
SET duration_minutes = SPLIT_PART(duration, ' ', 1)::INT
WHERE type = 'Movie';

/* TV Shows */
UPDATE netflix_raw
SET seasons = SPLIT_PART(duration, ' ', 1)::INT
WHERE type = 'TV Show';


/* =========================================================
   STEP 5: TEXT STANDARDIZATION
   ========================================================= */

UPDATE netflix_raw
SET 
    title = TRIM(title),
    director = TRIM(director),
    cast_members = TRIM(cast_members),
    country = TRIM(country),
    listed_in = TRIM(listed_in);


/* =========================================================
   STEP 6: HANDLE NULL VALUES
   ========================================================= */

UPDATE netflix_raw
SET director = 'Unknown'
WHERE director IS NULL;

UPDATE netflix_raw
SET country = 'Unknown'
WHERE country IS NULL;

UPDATE netflix_raw
SET cast_members = 'Unknown'
WHERE cast_members IS NULL;


/* =========================================================
   STEP 7: VALIDATION CHECKS
   ========================================================= */

/* Check cleaned date */
SELECT date_added, date_added_clean
FROM netflix_raw
LIMIT 10;

/* Check duration split */
SELECT title, type, duration, duration_minutes, seasons
FROM netflix_raw
LIMIT 10;

/* Check null handling */
SELECT *
FROM netflix_raw
WHERE director = 'Unknown'
LIMIT 10;

/* =========================================================
   PHASE 3: DATABASE NORMALIZATION
   Objective:
   Convert denormalized raw data into structured tables

   Why:
   - Improves performance
   - Enables efficient joins
   - Matches real-world database design

   Key Concept:
   1 row ≠ multiple values in a column
   ========================================================= */


/*PHASE 3: NORMALIZATION (REAL DATABASE DESIGN)

---WHY NORMALIZATION? 
Right now your data looks like this
cast_members = "Actor1, Actor2, Actor3"
listed_in = "Drama, Comedy"
country = "India, USA"

This is BAD for:
Searching
Joining
Analytics

We will convert this into:

Tables:
titles (main table)
actors
genres
countries
Mapping Tables:
title_actor_map
title_genre_map
title_country_map

👉 This is EXACTLY how companies design databases. */

/* =========================================================
   STEP 1: CREATE TITLES TABLE

   Objective:
   Extract core attributes into a clean main table

   Why:
   - Acts as the central fact table
   - Removes unnecessary raw columns

   Approach:
   Use CREATE TABLE AS SELECT for fast creation

   Alternative:
   Could use INSERT INTO after creating structure manually
   ========================================================= */

DROP TABLE IF EXISTS titles;

CREATE TABLE titles AS
SELECT 
    show_id,
    type,
    title,
    director,
    date_added_clean,
    year_added,
    month_added,
    release_year,
    rating,
    duration_minutes,
    seasons,
    description
FROM netflix_raw;


/*
Explanation of Output:

NOTICE: table "titles" does not exist, skipping
→ Safe drop attempt (no error)

SELECT 8807
→ 8807 rows inserted into titles table
*/


/* Validation */
SELECT COUNT(*) AS total_titles FROM titles;


/* =========================================================
   STEP: CREATE TITLES TABLE
   ========================================================= */

/*
Explanation of Output Messages:

1. NOTICE: table "titles" does not exist, skipping

- This message comes from:
    DROP TABLE IF EXISTS titles;

- Meaning:
    PostgreSQL attempted to drop the table "titles"
    but it did not exist yet.

- Result:
    No error occurred, operation skipped safely.

- Why we use this:
    Prevents errors when re-running scripts
    (Best practice in real-world projects)


2. SELECT 8807

- This comes from:
    CREATE TABLE titles AS SELECT ...

- Meaning:
    8807 rows were successfully inserted into the new table

- Insight:
    This confirms that:
    ✔ Data extraction worked correctly
    ✔ Table creation was successful
*/


/*
Validation Step:

Always verify row count after table creation
to ensure no data loss occurred
*/

SELECT COUNT(*) AS total_rows
FROM titles;


/*
Expected Output:
- Around 8807 rows (same as raw dataset)

Why this matters:
- Confirms data integrity
- Ensures transformation did not drop records
*/


/*
Next Steps Reminder:

After creating titles table, we must create:

1. actors table
2. genres table
3. countries table
4. mapping tables

Why:
- To normalize the database
- To enable efficient joins and analysis

Without this:
- Queries will be slow and complex
- Analysis will be limited
*/

/* =========================================================
   STEP 2: CREATE ACTORS TABLE

   Problem:
   - cast_members contains multiple actors in one column

   Example:
   "Actor1, Actor2, Actor3"

   Solution:
   - Split using STRING_TO_ARRAY
   - Expand using UNNEST
   - Clean using TRIM

   Why LATERAL:
   - Allows row-wise expansion of arrays

   Result:
   1 actor per row
   ========================================================= */

DROP TABLE IF EXISTS actors;

CREATE TABLE actors AS
SELECT DISTINCT 
    TRIM(actor) AS actor_name
FROM netflix_raw,
LATERAL UNNEST(STRING_TO_ARRAY(cast_members, ',')) AS actor;


/*
Why DISTINCT?
- Removes duplicate actor names

Alternative:
- Could create ID-based table (advanced step later)
*/


/* Validation */
SELECT COUNT(*) AS total_actors FROM actors;
SELECT * FROM actors LIMIT 10;

/* =========================================================
   STEP 3: CREATE GENRES TABLE

   Problem:
   - Multiple genres in one column

   Example:
   "Drama, Comedy"

   Solution:
   - Same approach as actors
   ========================================================= */

DROP TABLE IF EXISTS genres;

CREATE TABLE genres AS
SELECT DISTINCT 
    TRIM(genre) AS genre_name
FROM netflix_raw,
LATERAL UNNEST(STRING_TO_ARRAY(listed_in, ',')) AS genre;


/* Validation */
SELECT COUNT(*) AS total_genres FROM genres;
SELECT * FROM genres LIMIT 10;

/* =========================================================
   STEP 4: CREATE COUNTRIES TABLE

   Problem:
   - Multiple countries in one column

   Example:
   "India, USA"

   Solution:
   - Split and normalize
   ========================================================= */

DROP TABLE IF EXISTS countries;

CREATE TABLE countries AS
SELECT DISTINCT 
    TRIM(country_name) AS country_name
FROM netflix_raw,
LATERAL UNNEST(STRING_TO_ARRAY(country, ',')) AS country_name;


/* Validation */
SELECT COUNT(*) AS total_countries FROM countries;
SELECT * FROM countries LIMIT 10;

/* =========================================================
   PHASE 5: PERFORMANCE OPTIMIZATION (PRO LEVEL)

   Objective:
   Improve query performance using indexing and optimization

   Why:
   - Faster query execution
   - Efficient joins
   - Scalable system design

   Key Concepts:
   - Indexing
   - Query planning
   - Execution optimization
   ========================================================= */


/* =========================================================
   STEP 1: PRIMARY KEY CONSTRAINTS

   Why:
   - Ensures uniqueness
   - Automatically creates index
   - Improves join performance
   ========================================================= */

ALTER TABLE titles
ADD CONSTRAINT pk_titles PRIMARY KEY (show_id);


/*
Actors, genres, countries are dimension tables
We enforce uniqueness
*/

ALTER TABLE actors
ADD CONSTRAINT pk_actors PRIMARY KEY (actor_name);

ALTER TABLE genres
ADD CONSTRAINT pk_genres PRIMARY KEY (genre_name);

ALTER TABLE countries
ADD CONSTRAINT pk_countries PRIMARY KEY (country_name);


/* =========================================================
   STEP 2: INDEXES FOR JOINS (CRITICAL)

   Why:
   - Joins are the most expensive operations
   - Index on foreign keys speeds them up massively
   ========================================================= */

CREATE INDEX idx_title_actor_show_id 
ON title_actor_map(show_id);

CREATE INDEX idx_title_genre_show_id 
ON title_genre_map(show_id);

CREATE INDEX idx_title_country_show_id 
ON title_country_map(show_id);


/* =========================================================
   STEP 3: INDEXES FOR FILTERING

   Why:
   - Used in WHERE conditions
   - Speeds up filtering queries
   ========================================================= */

CREATE INDEX idx_titles_type 
ON titles(type);

CREATE INDEX idx_titles_year 
ON titles(year_added);

CREATE INDEX idx_titles_director 
ON titles(director);


/* =========================================================
   STEP 4: COMPOSITE INDEXES (ADVANCED)

   Why:
   - Used when multiple columns appear together
   - Great for analytics queries
   ========================================================= */

CREATE INDEX idx_titles_type_year
ON titles(type, year_added);


/*
Used in queries like:
WHERE type = 'Movie' AND year_added = 2020
*/


/* =========================================================
   STEP 5: TEXT SEARCH OPTIMIZATION (ADVANCED)

   Problem:
   Searching title/description is slow with LIKE

   Solution:
   Use FULL-TEXT SEARCH
   ========================================================= */

ALTER TABLE titles
ADD COLUMN search_vector tsvector;

UPDATE titles
SET search_vector =
    to_tsvector('english', title || ' ' || description);


/* Create GIN index for full-text search */

CREATE INDEX idx_titles_search 
ON titles USING GIN(search_vector);


/*
Example optimized query:

SELECT title
FROM titles
WHERE search_vector @@ to_tsquery('action');
*/


/* =========================================================
   STEP 6: ANALYZE TABLE (QUERY PLANNER)

   Why:
   - Helps PostgreSQL understand data distribution
   - Improves execution plan
   ========================================================= */

ANALYZE titles;
ANALYZE title_actor_map;
ANALYZE title_genre_map;
ANALYZE title_country_map;


/* =========================================================
   STEP 7: EXPLAIN ANALYSIS (DEBUG PERFORMANCE)

   Why:
   - Shows how query executes
   - Helps identify bottlenecks
   ========================================================= */

/*
Example:
*/

EXPLAIN ANALYZE
SELECT t.title, tam.actor_name
FROM titles t
JOIN title_actor_map tam
ON t.show_id = tam.show_id;


/*
Look for:
- Seq Scan (slow on large data)
- Index Scan (fast ✅)
*/


/* =========================================================
   STEP 8: MATERIALIZED VIEW (ADVANCED OPTIMIZATION)

   Objective:
   Speed up frequently used heavy queries

   Example:
   Top genres per year
   ========================================================= */

CREATE MATERIALIZED VIEW mv_genre_year AS
SELECT 
    t.year_added,
    g.genre_name,
    COUNT(*) AS total_titles
FROM titles t
JOIN title_genre_map g
    ON t.show_id = g.show_id
GROUP BY t.year_added, g.genre_name;


/*
Refresh when data updates:
*/

REFRESH MATERIALIZED VIEW mv_genre_year;


/*
Query becomes MUCH faster:
*/

SELECT *
FROM mv_genre_year
ORDER BY year_added, total_titles DESC;


/* =========================================================
   STEP 9: VACUUM (MAINTENANCE)

   Why:
   - Reclaims storage
   - Prevents table bloat
   ========================================================= */

VACUUM ANALYZE;


/* =========================================================
   STEP 10: OPTIONAL - PARTITIONING (VERY ADVANCED)

   When:
   - Dataset becomes VERY large (millions+ rows)

   Example Strategy:
   Partition by year_added

   NOTE:
   Not required for this dataset but used in real systems
   ========================================================= */

/*
Example (conceptual):

CREATE TABLE titles_partitioned (
    ...
) PARTITION BY RANGE (year_added);

*/


/* =========================================================
   STEP 5A: TITLE-ACTOR MAPPING

   Objective:
   Create many-to-many relationship

   Why:
   - One title → many actors
   - One actor → many titles

   Result:
   Bridge table for joins
   ========================================================= */

DROP TABLE IF EXISTS title_actor_map;

CREATE TABLE title_actor_map AS
SELECT 
    show_id,
    TRIM(actor) AS actor_name
FROM netflix_raw,
LATERAL UNNEST(STRING_TO_ARRAY(cast_members, ',')) AS actor;


/* Validation */
SELECT COUNT(*) FROM title_actor_map;
SELECT * FROM title_actor_map LIMIT 10;

/* =========================================================
   STEP 5B: TITLE-GENRE MAPPING
   ========================================================= */

DROP TABLE IF EXISTS title_genre_map;

CREATE TABLE title_genre_map AS
SELECT 
    show_id,
    TRIM(genre) AS genre_name
FROM netflix_raw,
LATERAL UNNEST(STRING_TO_ARRAY(listed_in, ',')) AS genre;


/* Validation */
SELECT COUNT(*) FROM title_genre_map;
SELECT * FROM title_genre_map LIMIT 10;

/* =========================================================
   STEP 5C: TITLE-COUNTRY MAPPING
   ========================================================= */

DROP TABLE IF EXISTS title_country_map;

CREATE TABLE title_country_map AS
SELECT 
    show_id,
    TRIM(country_name) AS country_name
FROM netflix_raw,
LATERAL UNNEST(STRING_TO_ARRAY(country, ',')) AS country_name;


/* Validation */
SELECT COUNT(*) FROM title_country_map;
SELECT * FROM title_country_map LIMIT 10;

/* =========================================================
   FINAL VALIDATION

   Objective:
   Ensure all tables are created correctly
   ========================================================= */

SELECT COUNT(*) FROM titles;
SELECT COUNT(*) FROM actors;
SELECT COUNT(*) FROM genres;
SELECT COUNT(*) FROM countries;

SELECT COUNT(*) FROM title_actor_map;
SELECT COUNT(*) FROM title_genre_map;
SELECT COUNT(*) FROM title_country_map;

----Data cleaning and Normalizing is done
---Running basic queries

/* =========================================================
   QUERY 1: TOTAL NUMBER OF TITLES

   Objective:
   Find total number of records in titles table

   Why:
   - Basic sanity check
   - Used as KPI in dashboards

   Alternative:
   COUNT(show_id)
   ========================================================= */

SELECT COUNT(*) AS total_titles
FROM titles;

/* =========================================================
   QUERY 2: CONTENT DISTRIBUTION BY TYPE

   Objective:
   Count how many Movies vs TV Shows

   Why:
   - Understand platform content mix
   - Business insight for strategy

   Alternative:
   Use CASE WHEN for custom grouping
   ========================================================= */

SELECT 
    type,
    COUNT(*) AS total
FROM titles
GROUP BY type
ORDER BY total DESC;

/* =========================================================
   QUERY 3: YEARLY CONTENT ADDITION

   Objective:
   Analyze growth trend of Netflix content

   Why:
   - Identify expansion phases
   - Useful for time-series analysis

   Alternative:
   Use date_trunc('year', date_added_clean)
   ========================================================= */

SELECT 
    year_added,
    COUNT(*) AS total_titles
FROM titles
GROUP BY year_added
ORDER BY year_added;

/* =========================================================
   QUERY 4: TOP CONTENT PRODUCING COUNTRIES

   Objective:
   Identify which countries produce most content

   Why:
   - Helps understand market focus
   - Important for business expansion

   NOTE:
   Using normalized table (BEST PRACTICE)
   ========================================================= */

SELECT 
    tcm.country_name,
    COUNT(*) AS total_titles
FROM title_country_map tcm
GROUP BY tcm.country_name
ORDER BY total_titles DESC
LIMIT 10;

/* =========================================================
   QUERY 5: MOST FREQUENT ACTORS

   Objective:
   Find actors appearing in most titles

   Why:
   - Identify popular actors
   - Useful for recommendation systems
   ========================================================= */

SELECT 
    tam.actor_name,
    COUNT(*) AS total_titles
FROM title_actor_map tam
GROUP BY tam.actor_name
ORDER BY total_titles DESC
LIMIT 10;

/* =========================================================
   QUERY 6: MOST POPULAR GENRES

   Objective:
   Identify dominant genres

   Why:
   - Content strategy
   - Audience preference insight
   ========================================================= */

SELECT 
    tgm.genre_name,
    COUNT(*) AS total_titles
FROM title_genre_map tgm
GROUP BY tgm.genre_name
ORDER BY total_titles DESC
LIMIT 10;

/* P4 LEVEL 2: JOINS */


/* =========================================================
   QUERY 1: GET TITLES WITH THEIR ACTORS

   Objective:
   Combine titles with actors

   Join Type:
   INNER JOIN

   Why:
   Only want titles that have actors mapped

   Alternative:
   LEFT JOIN → to include titles without actors
   ========================================================= */

SELECT 
    t.title,
    tam.actor_name
FROM titles t
INNER JOIN title_actor_map tam
ON t.show_id = tam.show_id
LIMIT 20;

/* =========================================================
   QUERY 2: MULTI-TABLE JOIN

   Objective:
   Combine title with genre and country

   Why:
   Real-world queries often involve multiple joins

   Important:
   Avoid duplicate confusion by understanding relationships
   ========================================================= */

SELECT 
    t.title,
    t.type,
    tgm.genre_name,
    tcm.country_name
FROM titles t
INNER JOIN title_genre_map tgm
    ON t.show_id = tgm.show_id
INNER JOIN title_country_map tcm
    ON t.show_id = tcm.show_id
LIMIT 20;

/* =========================================================
   QUERY 3: FILTER + JOIN

   Objective:
   Find top actors only in Movies

   Why:
   Combine filtering + joins

   Key Concept:
   WHERE applied after join
   ========================================================= */

SELECT 
    tam.actor_name,
    COUNT(*) AS movie_count
FROM titles t
INNER JOIN title_actor_map tam
    ON t.show_id = tam.show_id
WHERE t.type = 'Movie'
GROUP BY tam.actor_name
ORDER BY movie_count DESC
LIMIT 10;

/* =========================================================
   QUERY 4: LEFT JOIN USAGE

   Objective:
   Include all titles even if no actor exists

   Why:
   INNER JOIN removes unmatched rows
   LEFT JOIN keeps them

   Use Case:
   Data completeness analysis
   ========================================================= */

SELECT 
    t.title,
    tam.actor_name
FROM titles t
LEFT JOIN title_actor_map tam
    ON t.show_id = tam.show_id
LIMIT 20;

/* =========================================================
   QUERY 5: ADVANCED JOIN + GROUPING

   Objective:
   Analyze genre trends over time

   Why:
   Real business question:
   "Which genres are growing?"

   Key Learning:
   Multi-column GROUP BY
   ========================================================= */

SELECT 
    t.year_added,
    tgm.genre_name,
    COUNT(*) AS total_titles
FROM titles t
INNER JOIN title_genre_map tgm
    ON t.show_id = tgm.show_id
GROUP BY t.year_added, tgm.genre_name
ORDER BY t.year_added, total_titles DESC;


/* =========================================================
   QUERY 6: FILTER + JOIN + GROUP

   Objective:
   Find number of movies per country

   Why:
   Market-level analysis
   ========================================================= */

SELECT 
    tcm.country_name,
    COUNT(*) AS total_movies
FROM titles t
INNER JOIN title_country_map tcm
    ON t.show_id = tcm.show_id
WHERE t.type = 'Movie'
GROUP BY tcm.country_name
ORDER BY total_movies DESC
LIMIT 10;

/* P4 LEVEL 3: Advanced SQL */

/* =========================================================
   QUERY 1: TOP 3 ACTORS PER YEAR

   Objective:
   Find most active actors each year

   Concepts:
   - JOIN
   - GROUP BY
   - WINDOW FUNCTION (RANK)

   Why RANK():
   Handles ties properly

   Alternative:
   ROW_NUMBER() → strict ranking (no ties)
   ========================================================= */

WITH actor_year_count AS (
    SELECT 
        t.year_added,
        tam.actor_name,
        COUNT(*) AS total_titles
    FROM titles t
    JOIN title_actor_map tam
        ON t.show_id = tam.show_id
    GROUP BY t.year_added, tam.actor_name
),

ranked_actors AS (
    SELECT *,
        RANK() OVER (
            PARTITION BY year_added
            ORDER BY total_titles DESC
        ) AS rank
    FROM actor_year_count
)

SELECT *
FROM ranked_actors
WHERE rank <= 3;

/* =========================================================
   QUERY 2: TOP GENRE PER YEAR

   Objective:
   Identify most popular genre each year

   Concepts:
   - CTE
   - GROUP BY
   - ROW_NUMBER()

   Why ROW_NUMBER():
   Forces single top result per year
   ========================================================= */

WITH genre_year AS (
    SELECT 
        t.year_added,
        tgm.genre_name,
        COUNT(*) AS total_titles
    FROM titles t
    JOIN title_genre_map tgm
        ON t.show_id = tgm.show_id
    GROUP BY t.year_added, tgm.genre_name
),

ranked_genres AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY year_added
            ORDER BY total_titles DESC
        ) AS rank
    FROM genre_year
)

SELECT *
FROM ranked_genres
WHERE rank = 1;

/* =========================================================
   QUERY 3: RUNNING TOTAL

   Objective:
   Track cumulative content growth

   Concepts:
   - WINDOW FUNCTION
   - SUM OVER()

   Why:
   Shows growth trend clearly
   ========================================================= */

SELECT 
    year_added,
    COUNT(*) AS yearly_titles,
    SUM(COUNT(*)) OVER (ORDER BY year_added) AS running_total
FROM titles
GROUP BY year_added
ORDER BY year_added;

/* =========================================================
   QUERY 4: DUPLICATE DETECTION

   Objective:
   Identify duplicate titles

   Concepts:
   - GROUP BY
   - HAVING

   Why:
   Data quality check
   ========================================================= */

SELECT 
    title,
    COUNT(*) AS occurrences
FROM titles
GROUP BY title
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

/* =========================================================
   QUERY 5: SUBQUERY USAGE

   Objective:
   Find movies longer than average duration

   Concepts:
   - Subquery
   - Aggregation

   Alternative:
   Use CTE instead
   ========================================================= */

SELECT title, duration_minutes
FROM titles
WHERE duration_minutes > (
    SELECT AVG(duration_minutes)
    FROM titles
    WHERE type = 'Movie'
);

/* =========================================================
   QUERY 6: DIRECTOR CONSISTENCY

   Objective:
   Find directors with most content

   Why:
   Business insight → content creators

   Advanced:
   Can later combine with ratings
   ========================================================= */

SELECT 
    director,
    COUNT(*) AS total_titles
FROM titles
GROUP BY director
ORDER BY total_titles DESC
LIMIT 10;

/* P4 LEVEL 4: BUSINESS CASES */

/* =========================================================
   CASE 1: FASTEST GROWING GENRE

   Objective:
   Identify which genre is growing fastest year-over-year

   Business Use:
   Helps Netflix decide where to invest

   Concepts:
   - CTE
   - LAG()
   - Growth calculation
   ========================================================= */

WITH genre_year AS (
    SELECT 
        t.year_added,
        tgm.genre_name,
        COUNT(*) AS total_titles
    FROM titles t
    JOIN title_genre_map tgm
        ON t.show_id = tgm.show_id
    GROUP BY t.year_added, tgm.genre_name
),

growth_calc AS (
    SELECT *,
        LAG(total_titles) OVER (
            PARTITION BY genre_name
            ORDER BY year_added
        ) AS prev_year
    FROM genre_year
)

SELECT 
    genre_name,
    year_added,
    total_titles,
    prev_year,
    (total_titles - prev_year) AS growth
FROM growth_calc
WHERE prev_year IS NOT NULL
ORDER BY growth DESC
LIMIT 10;

/* =========================================================
   CASE 2: BINGE-WORTHY SHOWS

   Objective:
   Find shows with highest number of seasons

   Business Use:
   Identify high-engagement content

   Insight:
   More seasons = higher retention potential
   ========================================================= */

SELECT 
    title,
    seasons
FROM titles
WHERE type = 'TV Show'
ORDER BY seasons DESC
LIMIT 10;


/* =========================================================
   CASE 3: MULTI-COUNTRY CONTENT

   Objective:
   Identify content produced in multiple countries

   Business Use:
   Global collaboration insights
   ========================================================= */

SELECT 
    t.title,
    COUNT(tcm.country_name) AS country_count
FROM titles t
JOIN title_country_map tcm
    ON t.show_id = tcm.show_id
GROUP BY t.title
HAVING COUNT(tcm.country_name) > 1
ORDER BY country_count DESC;


/* =========================================================
   CASE 4: MOST PRODUCTIVE DIRECTORS

   Objective:
   Find directors with consistent output

   Business Use:
   Identify reliable content creators

   Advanced:
   Later can combine with ratings dataset
   ========================================================= */

SELECT 
    director,
    COUNT(*) AS total_titles
FROM titles
WHERE director != 'Unknown'
GROUP BY director
ORDER BY total_titles DESC
LIMIT 10;


/* =========================================================
   CASE 5: CONTENT GAP

   Objective:
   Identify underrepresented genres

   Business Use:
   Find opportunity areas for new content
   ========================================================= */

SELECT 
    genre_name,
    COUNT(*) AS total_titles
FROM title_genre_map
GROUP BY genre_name
ORDER BY total_titles ASC
LIMIT 10;

/* =========================================================
   CASE 6: ACTOR COLLABORATION

   Objective:
   Find actors who frequently appear together

   Concepts:
   - Self join
   - Advanced join logic

   Business Use:
   Casting strategy insights
   ========================================================= */

SELECT 
    a1.actor_name AS actor_1,
    a2.actor_name AS actor_2,
    COUNT(*) AS collaborations
FROM title_actor_map a1
JOIN title_actor_map a2
    ON a1.show_id = a2.show_id
    AND a1.actor_name < a2.actor_name
GROUP BY actor_1, actor_2
ORDER BY collaborations DESC
LIMIT 10;

/* =========================================================
   PHASE 6: STAR SCHEMA (DATA WAREHOUSE DESIGN)

   Objective:
   Transform normalized tables into a Star Schema

   Why:
   - Optimized for analytics (BI tools like Power BI/Tableau)
   - Faster aggregations
   - Simpler queries (fewer joins)

   Key Concept:
   STAR SCHEMA = FACT TABLE + DIMENSION TABLES

   Structure:

           dim_actor
                |
   dim_date — fact_titles — dim_genre
                |
           dim_country

   ========================================================= */


/* =========================================================
   DIMENSION: DATE

   Why:
   - Enables time-based analysis
   - Standard in all data warehouses
   ========================================================= */

DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date AS
SELECT DISTINCT
    date_added_clean AS full_date,
    EXTRACT(YEAR FROM date_added_clean) AS year,
    EXTRACT(MONTH FROM date_added_clean) AS month,
    TO_CHAR(date_added_clean, 'Month') AS month_name
FROM titles
WHERE date_added_clean IS NOT NULL;


/* Add surrogate key */

ALTER TABLE dim_date
ADD COLUMN date_id SERIAL PRIMARY KEY;


/* =========================================================
   DIMENSION: ACTOR
   ========================================================= */

DROP TABLE IF EXISTS dim_actor;

CREATE TABLE dim_actor AS
SELECT DISTINCT actor_name
FROM actors;

ALTER TABLE dim_actor
ADD COLUMN actor_id SERIAL PRIMARY KEY;


/* =========================================================
   DIMENSION: GENRE
   ========================================================= */

DROP TABLE IF EXISTS dim_genre;

CREATE TABLE dim_genre AS
SELECT DISTINCT genre_name
FROM genres;

ALTER TABLE dim_genre
ADD COLUMN genre_id SERIAL PRIMARY KEY;

/* =========================================================
   DIMENSION: COUNTRY
   ========================================================= */

DROP TABLE IF EXISTS dim_country;

CREATE TABLE dim_country AS
SELECT DISTINCT country_name
FROM countries;

ALTER TABLE dim_country
ADD COLUMN country_id SERIAL PRIMARY KEY;


/* =========================================================
   DIMENSION: CONTENT TYPE

   Why:
   Avoid repeated TEXT values like "Movie", "TV Show"
   ========================================================= */

DROP TABLE IF EXISTS dim_type;

CREATE TABLE dim_type AS
SELECT DISTINCT type
FROM titles;

ALTER TABLE dim_type
ADD COLUMN type_id SERIAL PRIMARY KEY;


/* =========================================================
   FACT TABLE: FACT_TITLES

   Grain:
   One row per title

   Why:
   - Central table for analytics
   - Contains measurable attributes

   Measures:
   - duration_minutes
   - seasons

   Foreign Keys:
   - date_id
   - type_id

   ========================================================= */

DROP TABLE IF EXISTS fact_titles;

CREATE TABLE fact_titles AS
SELECT 
    t.show_id,
    t.title,
    t.release_year,
    t.duration_minutes,
    t.seasons,

    d.date_id,
    ty.type_id

FROM titles t
LEFT JOIN dim_date d
    ON t.date_added_clean = d.full_date
LEFT JOIN dim_type ty
    ON t.type = ty.type;


/* =========================================================
   BRIDGE: FACT ↔ ACTOR
   ========================================================= */

DROP TABLE IF EXISTS bridge_title_actor;

CREATE TABLE bridge_title_actor AS
SELECT 
    tam.show_id,
    da.actor_id
FROM title_actor_map tam
JOIN dim_actor da
    ON tam.actor_name = da.actor_name;


/* =========================================================
   BRIDGE: FACT ↔ GENRE
   ========================================================= */

DROP TABLE IF EXISTS bridge_title_genre;

CREATE TABLE bridge_title_genre AS
SELECT 
    tgm.show_id,
    dg.genre_id
FROM title_genre_map tgm
JOIN dim_genre dg
    ON tgm.genre_name = dg.genre_name;


/* =========================================================
   BRIDGE: FACT ↔ COUNTRY
   ========================================================= */

DROP TABLE IF EXISTS bridge_title_country;

CREATE TABLE bridge_title_country AS
SELECT 
    tcm.show_id,
    dc.country_id
FROM title_country_map tcm
JOIN dim_country dc
    ON tcm.country_name = dc.country_name;


/* =========================================================
   INDEXING STAR SCHEMA

   Why:
   - Faster joins in BI tools
   ========================================================= */

CREATE INDEX idx_fact_date ON fact_titles(date_id);
CREATE INDEX idx_fact_type ON fact_titles(type_id);

CREATE INDEX idx_bridge_actor ON bridge_title_actor(actor_id);
CREATE INDEX idx_bridge_genre ON bridge_title_genre(genre_id);
CREATE INDEX idx_bridge_country ON bridge_title_country(country_id);

---Movies per Year

SELECT 
    d.year,
    COUNT(*) AS total_titles
FROM fact_titles f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year
ORDER BY d.year;

---Top Genres

SELECT 
    g.genre_name,
    COUNT(*) AS total
FROM fact_titles f
JOIN bridge_title_genre bg ON f.show_id = bg.show_id
JOIN dim_genre g ON bg.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY total DESC;

---Country Analysis

SELECT 
    c.country_name,
    COUNT(*) AS total
FROM fact_titles f
JOIN bridge_title_country bc ON f.show_id = bc.show_id
JOIN dim_country c ON bc.country_id = c.country_id
GROUP BY c.country_name
ORDER BY total DESC;



