# 🧩 PHASE 3: DATABASE NORMALIZATION

---

## 📌 Objective

Convert denormalized raw Netflix dataset into a structured relational database.

---

## ❓ Why Normalization?

- Improves performance
- Enables efficient joins
- Matches real-world database design

---

## 🧠 Key Concept

> 1 row ≠ multiple values in a column

---

## ⚠️ Problem in Raw Data

```sql
cast_members = "Actor1, Actor2, Actor3"
listed_in = "Drama, Comedy"
country = "India, USA"
```

### ❌ Issues:
- Difficult to search
- Hard to join tables
- Poor analytics performance

---

## 🏗️ Target Structure

We convert into:

### 📊 Tables:
- titles (main table)
- actors
- genres
- countries

### 🔗 Mapping Tables (used later):
- title_actor_map
- title_genre_map
- title_country_map

---

# 🏷️ STEP 1: CREATE TITLES TABLE

```sql
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
```

---

## 📤 Output Explanation

```
NOTICE: table "titles" does not exist, skipping
→ Safe drop attempt (no error)

SELECT 8807
→ 8807 rows inserted
```

---

## ✅ Validation

```sql
SELECT COUNT(*) AS total_titles FROM titles;
```

```sql
SELECT COUNT(*) AS total_rows
FROM titles;
```

---

## 📌 Why This Matters

- Ensures data integrity
- Confirms full data migration
- Central table for analytics

---

## 📍 Next Step Reminder

We now create:

- actors
- genres
- countries
- mapping tables

---

# 🎭 STEP 2: CREATE ACTORS TABLE

## ❗ Problem

```sql
cast_members = "Actor1, Actor2, Actor3"
```

---

## 💡 Solution

- Split values using STRING_TO_ARRAY
- Expand using UNNEST
- Clean using TRIM

---

## ⚙️ Query

```sql
DROP TABLE IF EXISTS actors;

CREATE TABLE actors AS
SELECT DISTINCT 
    TRIM(actor) AS actor_name
FROM netflix_raw,
LATERAL UNNEST(STRING_TO_ARRAY(cast_members, ',')) AS actor;
```

---

## ✅ Validation

```sql
SELECT COUNT(*) AS total_actors FROM actors;
SELECT * FROM actors LIMIT 10;
```

---

# 🎬 STEP 3: CREATE GENRES TABLE

## ❗ Problem

```sql
listed_in = "Drama, Comedy"
```

---

## 💡 Solution

Same logic as actors:
- Split
- Expand
- Clean

---

## ⚙️ Query

```sql
DROP TABLE IF EXISTS genres;

CREATE TABLE genres AS
SELECT DISTINCT 
    TRIM(genre) AS genre_name
FROM netflix_raw,
LATERAL UNNEST(STRING_TO_ARRAY(listed_in, ',')) AS genre;
```

---

## ✅ Validation

```sql
SELECT COUNT(*) AS total_genres FROM genres;
SELECT * FROM genres LIMIT 10;
```

---

# 🌍 STEP 4: CREATE COUNTRIES TABLE

## ❗ Problem

```sql
country = "India, USA"
```

---

## 💡 Solution

- Normalize multi-value field
- Create 1 country per row

---

## ⚙️ Query

```sql
DROP TABLE IF EXISTS countries;

CREATE TABLE countries AS
SELECT DISTINCT 
    TRIM(country_name) AS country_name
FROM netflix_raw,
LATERAL UNNEST(STRING_TO_ARRAY(country, ',')) AS country_name;
```

---

## ✅ Validation

```sql
SELECT COUNT(*) AS total_countries FROM countries;
SELECT * FROM countries LIMIT 10;
```

---

# 📌 FINAL RESULT OF PHASE 3

## ✔ You achieved:

- Fully normalized database (3NF)
- Clean separation of entities
- Ready for joins & analytics
- Scalable database design

---

## 🚀 What this enables next:

- Mapping tables (many-to-many relationships)
- Advanced SQL analytics
- Star schema design (Data warehouse layer)
