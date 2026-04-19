# 🚀 PHASE 2: DATA CLEANING (COMPLETE SYSTEM SCRIPT)

---

## =========================================================
## PHASE 2: DATA CLEANING & PREPARATION
## Objective: Transform raw Netflix data into clean dataset
## =========================================================

---

# 📊 STEP 1: DATA QUALITY AUDIT

```sql
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
```

---

# 📅 STEP 2: CLEAN DATE COLUMN

## Problem: Stored as TEXT  
## Solution: Convert to DATE

```sql
ALTER TABLE netflix_raw
ADD COLUMN date_added_clean DATE;

UPDATE netflix_raw
SET date_added_clean = TO_DATE(date_added, 'Month DD, YYYY');
```

---

# 📆 STEP 3: FEATURE ENGINEERING (DATE)

```sql
ALTER TABLE netflix_raw
ADD COLUMN year_added INT,
ADD COLUMN month_added INT;

UPDATE netflix_raw
SET 
    year_added = EXTRACT(YEAR FROM date_added_clean),
    month_added = EXTRACT(MONTH FROM date_added_clean);
```

---

# 🎬 STEP 4: CLEAN DURATION COLUMN

```sql
ALTER TABLE netflix_raw
ADD COLUMN duration_minutes INT,
ADD COLUMN seasons INT;
```

---

## 🎥 Movies Duration

```sql
UPDATE netflix_raw
SET duration_minutes = SPLIT_PART(duration, ' ', 1)::INT
WHERE type = 'Movie';
```

---

## 📺 TV Shows Duration

```sql
UPDATE netflix_raw
SET seasons = SPLIT_PART(duration, ' ', 1)::INT
WHERE type = 'TV Show';
```

---

# 🧹 STEP 5: TEXT STANDARDIZATION

```sql
UPDATE netflix_raw
SET 
    title = TRIM(title),
    director = TRIM(director),
    cast_members = TRIM(cast_members),
    country = TRIM(country),
    listed_in = TRIM(listed_in);
```

---

# ⚠️ STEP 6: HANDLE NULL VALUES

```sql
UPDATE netflix_raw
SET director = 'Unknown'
WHERE director IS NULL;

UPDATE netflix_raw
SET country = 'Unknown'
WHERE country IS NULL;

UPDATE netflix_raw
SET cast_members = 'Unknown'
WHERE cast_members IS NULL;
```

---

# ✅ STEP 7: VALIDATION CHECKS

---

## 📌 Check Cleaned Date

```sql
SELECT date_added, date_added_clean
FROM netflix_raw
LIMIT 10;
```

---

## 📌 Check Duration Split

```sql
SELECT title, type, duration, duration_minutes, seasons
FROM netflix_raw
LIMIT 10;
```

---

## 📌 Check NULL Handling

```sql
SELECT *
FROM netflix_raw
WHERE director = 'Unknown'
LIMIT 10;
```

---

# 🎯 END OF PHASE 2

## ✔ Data cleaned  
## ✔ Dates standardized  
## ✔ Duration split  
## ✔ Missing values handled  
## ✔ Ready for normalization (Phase 3)

---
