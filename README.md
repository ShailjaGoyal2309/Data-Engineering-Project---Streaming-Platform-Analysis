# 🎬 Streaming Analytics Platform using Azure, Databricks & Power BI

## Project Overview

This project demonstrates a complete end-to-end Streaming Analytics Platform built on Microsoft Azure using the Medallion Architecture.

The platform simulates streaming platform data, automatically processes it using Azure Data Factory and Azure Databricks (PySpark), stores analytics-ready datasets in Azure Data Lake Storage Gen2, and visualizes insights through an interactive Power BI dashboard.

This project showcases cloud data engineering, ETL pipeline automation, big data processing, and business intelligence reporting.

---

# Project Objectives

- Simulate streaming platform activity
- Build an automated cloud data pipeline
- Implement Bronze, Silver and Gold data architecture
- Process large datasets using Apache Spark
- Build an interactive Power BI dashboard
- Generate actionable business insights

---

# Architecture

The project follows the Medallion Architecture.

```
Data Generation
        │
        ▼
Azure Data Lake (Raw)
        │
        ▼
Storage Event Trigger
        │
        ▼
Azure Data Factory
        │
        ▼
Azure Databricks
        │
 ┌──────┼──────┐
 ▼      ▼      ▼
Bronze Silver Gold
        │
        ▼
 Power BI Dashboard
```

Detailed architecture documentation is available in:

```
docs/architecture.txt 
```

---

# Technologies Used

- Microsoft Azure
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Apache Spark (PySpark)
- Python
- SQL
- Power BI
- Git
- GitHub

---

# Medallion Architecture

## Bronze Layer

- Raw data ingestion
- Source data preservation

---

## Silver Layer

- Data cleaning
- Validation
- Deduplication
- Standardization

---

## Gold Layer

Business-ready analytics tables:

- watch_completion_fact
- subscription_summary
- trending_genres
- movie_popularity
- user_genre_preference_matrix

---

# Dashboard Pages

## 1. Overview Dashboard

Provides high-level platform metrics including:

- Total Users
- Active Users
- Total Movies
- Total Watch Logs
- Total Watch Time
- Subscription Distribution
- Genre Popularity
- Movie Popularity

---

## 2. User Analytics

Analyzes user engagement.

Features:

- Premium Users
- User Watch Tier
- Watch Time by Subscription
- Top Active Users

---

## 3. Content Analytics

Provides insights into content performance.

Includes:

- Genre Popularity
- Movie Duration Analysis
- Release Year Trends
- Movie Engagement

---

## 4. Behavior Insights

Advanced analytical dashboard featuring:

- Completion Rate by Genre
- Viewer Activity Heatmap
- Returning vs New Users

---

# Pipeline Workflow

1. Run the Data Generation notebook.
2. Data is written to Azure Data Lake Storage (Raw).
3. Storage Event Trigger detects new files.
4. Azure Data Factory pipeline starts automatically.
5. Azure Databricks notebooks execute sequentially.
6. Bronze, Silver and Gold layers are generated.
7. Power BI refreshes to display the latest analytics.

---

# Repository Structure

```
Streaming-Analytics-Project
│
├── notebooks/
├── powerbi/
├── docs/
├── README.md
```

---

# Key Features

- End-to-End Azure Data Engineering Pipeline
- Automated Storage Event Trigger
- Cloud ETL Pipeline
- Apache Spark Data Processing
- Medallion Architecture
- Interactive Power BI Dashboard
- Dark Theme Dashboard Design
- Modular Databricks Notebooks
- Analytics-Ready Gold Layer
- GitHub Portfolio Ready

---

# Future Improvements

- Real-time streaming using Azure Event Hubs
- Delta Lake implementation
- Machine Learning recommendation engine
- Azure Synapse Analytics integration
- CI/CD pipeline using GitHub Actions
- Automated Power BI dataset refresh

---

# Learning Outcomes

Through this project, I gained practical experience with:

- Azure Cloud Services
- Data Engineering Pipelines
- Apache Spark
- PySpark Transformations
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Power BI Dashboard Development
- Cloud ETL Automation
- Data Modeling
- Business Intelligence

---

# Author

**Shailja Goyal**

Bachelor of Technology (B.Tech)

Data Engineering | Azure | PySpark | Power BI | SQL | Python

---

⭐ If you found this project helpful, consider giving the repository a star.
