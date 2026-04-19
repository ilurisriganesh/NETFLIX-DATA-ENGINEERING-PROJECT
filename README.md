# 🎬 Netflix Data Engineering & Analytics Project

## 📌 Overview

This project demonstrates an **end-to-end data engineering workflow** using a Netflix dataset — from raw data ingestion to a fully optimized **data warehouse (Star Schema)** ready for analytics and BI tools.

It follows industry-standard architecture:

* **Bronze Layer** → Raw Data
* **Silver Layer** → Cleaned & Normalized Data
* **Gold Layer** → Analytical Queries
* **Platinum Layer** → Performance Optimization
* **Warehouse Layer** → Star Schema (OLAP)

---

## 🚀 Project Objectives

* Build a real-world **data pipeline using SQL**
* Clean and transform messy raw data
* Normalize data into relational structure
* Design a **Star Schema for analytics**
* Optimize performance using indexing and advanced SQL
* Solve business problems using analytical queries

---

## 🏗️ Architecture Overview

![Architecture Diagram](https://github.com/ilurisriganesh/NETFLIX-DATA-ENGINEERING-PROJECT/blob/main/netflix%20star%20scheme.png)


```
            ┌────────────────────┐
            │   Raw CSV Dataset  │
            └─────────┬──────────┘
                      │
                      ▼
        ┌──────────────────────────┐
        │   BRONZE LAYER           │
        │   netflix_raw            │
        │   (Uncleaned Data)       │
        └─────────┬────────────────┘
                  │
                  ▼
        ┌──────────────────────────┐
        │   SILVER LAYER           │
        │   titles                 │
        │   actors                 │
        │   genres                 │
        │   countries              │
        │   + mapping tables       │
        └─────────┬────────────────┘
                  │
                  ▼
        ┌──────────────────────────┐
        │   GOLD LAYER             │
        │   Analytics Queries      │
        │   Business Insights      │
        └─────────┬────────────────┘
                  │
                  ▼
        ┌──────────────────────────┐
        │   PLATINUM LAYER         │
        │   Indexing               │
        │   Materialized Views     │
        │   Query Optimization     │
        └─────────┬────────────────┘
                  │
                  ▼
        ┌──────────────────────────┐
        │   DATA WAREHOUSE         │
        │   STAR SCHEMA            │
        │   fact_titles            │
        │   dim_* tables           │
        └──────────────────────────┘
```

---

## 🧱 Data Model

### ⭐ Star Schema Design

**Fact Table:**

* `fact_titles`

**Dimension Tables:**

* `dim_date`
* `dim_actor`
* `dim_genre`
* `dim_country`
* `dim_type`

**Bridge Tables (Many-to-Many):**

* `bridge_title_actor`
* `bridge_title_genre`
* `bridge_title_country`

---

## ⚙️ Tech Stack

* **Database:** PostgreSQL
* **Language:** SQL
* **Tools:** pgAdmin / DBeaver
* **Concepts Used:**

  * Data Cleaning & Transformation
  * Normalization (3NF)
  * Star Schema (OLAP)
  * Indexing & Query Optimization
  * Window Functions
  * CTEs & Subqueries
  * Materialized Views

---

## 🧹 Data Processing Steps

### 🔹 Phase 1: Raw Layer

* Ingested CSV into `netflix_raw`
* All columns stored as TEXT (schema-on-read approach)

### 🔹 Phase 2: Data Cleaning

* Converted dates to proper format
* Extracted year/month features
* Split duration into minutes/seasons
* Handled NULL values
* Standardized text fields

### 🔹 Phase 3: Normalization

* Created:

  * `titles`
  * `actors`, `genres`, `countries`
  * Mapping tables for relationships

### 🔹 Phase 4: Analytics

* Content distribution (Movies vs TV Shows)
* Yearly growth trends
* Top actors, genres, countries
* Advanced queries using:

  * Window functions
  * Ranking
  * Running totals

### 🔹 Phase 5: Optimization

* Indexes on joins and filters
* Composite indexes
* Full-text search using GIN index
* Materialized views for heavy queries

### 🔹 Phase 6: Data Warehouse

* Designed **Star Schema**
* Created fact + dimension tables
* Built bridge tables for many-to-many relationships

---

## 📊 Sample Business Questions Answered

* 📈 Which genre is growing the fastest?
* 🎭 Who are the most frequent actors?
* 🌍 Which countries produce the most content?
* 🎬 What are the longest-running TV shows?
* 🤝 Which actors collaborate most often?

---

## ⚡ Performance Optimization Highlights

* Added **Primary Keys & Indexes**
* Optimized joins using indexed columns
* Implemented **GIN index for full-text search**
* Used **Materialized Views** for faster analytics
* Applied **EXPLAIN ANALYZE** for query tuning

---

## 🧠 Key Learnings

* Real-world data is messy and requires cleaning
* Normalization improves structure but not always performance
* Star schema is ideal for analytics workloads
* Indexing is critical for scaling SQL systems
* Window functions unlock powerful insights

---

## 📌 Future Improvements

* Integrate with **Power BI / Tableau**
* Build ETL pipelines (Airflow)
* Add incremental data loading
* Implement partitioning for large datasets
* Convert into Snowflake schema

---

## 🙌 Author

I SRI GANESH

---

## ⭐ If you like this project

Give it a star ⭐ and feel free to fork!
