🛒 Retail Sales Analytics Project — Databricks
📌 Project Overview

This project builds an end-to-end Retail Sales Analytics solution using Databricks and the Medallion Architecture (Bronze → Silver → Gold).
It ingests messy retail data, cleans and transforms it, generates KPIs, and publishes an interactive Databricks SQL Dashboard for business insights.

🔗 Published Dashboard
https://dbc-ea88c990-5353.cloud.databricks.com/dashboardsv3/01f10c6edcec1ec3a2e13a7295a9d34a/published?o=2022441965144207

🧩 Business Use Case

A mid-size retail company struggles to consolidate and analyze sales data from multiple sources and formats.

🎯 Goals

Clean messy data (missing values, duplicates, inconsistent formats)

Create a single source of truth for sales, customers, and products

Track KPIs such as revenue, AOV, and repeat customers

Enable insights by product, category, and time

Provide dashboards for data-driven decision-making

🏗️ Architecture — Medallion Architecture

This project follows Databricks Medallion Architecture:

🥉 Bronze Layer — Raw Data

Purpose: Ingest raw data without transformations
Source: CSV / Parquet files (simulated in Databricks Community Edition)

Key Tasks

Load raw sales data

Store data in Delta tables

🥈 Silver Layer — Cleaned & Standardized Data

Purpose: Data cleaning and transformation

Transformations

Standardize date formats

Remove $ from price fields

Handle missing values

Remove duplicates

Normalize product names

Handle negative or invalid quantities

🥇 Gold Layer — Aggregated Business Data

Purpose: Create analytics-ready tables

Aggregations
Metrics grouped by:

Product

Customer

Category

Time (Year, Month, Date)

📊 KPIs Derived

Total Revenue

Total Orders

Total Items Sold

Average Order Value (AOV)

Average Price per Item

Repeat Customer Rate

Purchase Frequency

Return Rate

Monthly Sales Trends & MoM Growth

Top Products by Revenue & Quantity

Customer Lifetime Value (CLV) (approx.)

📈 Dashboard (Built in Databricks SQL)

The dashboard provides interactive insights.

Dashboard Features

Sales trends over time

Category performance

Top products

Customer insights

KPI summary tiles

📢 Published using Databricks Dashboard Sharing.

🛠️ Tech Stack

Databricks Community Edition

PySpark

Delta Lake

Databricks SQL

Medallion Architecture

Dashboard Publishing
