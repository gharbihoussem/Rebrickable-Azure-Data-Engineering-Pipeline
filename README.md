# 🚀 Rebrickable Azure Data Engineering Pipeline

## 📖 Overview

This project demonstrates an end-to-end Azure Data Engineering pipeline using Azure Data Factory, Databricks, and Azure DevOps.

The pipeline ingests LEGO Minifig data from the Rebrickable API, processes it, and stores analytics-ready datasets in ADLS Gen2.

---

## 🏗 Architecture

API → ADF → ADLS → Databricks → ADLS → CI/CD Deployment

(Add your diagram in /docs)

---

## ⚙️ Technologies Used

- Azure Data Factory
- Azure Databricks
- Azure Data Lake Gen2
- Azure Key Vault
- Microsoft Entra ID
- Azure Logic Apps
- Azure DevOps CI/CD
- PySpark & SQL

---

## 🔹 Features

### ✔ Secure Data Ingestion
- Rebrickable API ingestion
- API keys stored in Key Vault
- Managed Identity authentication

### ✔ Error Handling
- ADF On-Failure paths
- Logic App email alerts

### ✔ Data Processing
- Auto Loader for incremental load
- SQL transformations in Databricks

### ✔ Orchestration
- ADF triggers Databricks notebooks

### ✔ CI/CD
- Azure DevOps deployment pipelines

---

## 📊 Example Use Cases

- LEGO dataset analytics
- API ingestion pipeline template
- Secure enterprise pipeline design

---

## 🔖 About Me 
Hi there! I'm **Houssem Gharbi**, i'm a Junior Data Engineer passionate about building scalable data solutions and deriving actionable insights. I enjoy transforming raw data into meaningful stories that drive decision-making. This project reflects my skills and interest in creating efficient data pipelines and analytics platforms.


