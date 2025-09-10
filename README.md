# 📊 Loan Portfolio ETL Project  

## 📌 Overview  
This project is an **ETL (Extract, Transform, Load) pipeline** that integrates data from **two PostgreSQL databases** and **MongoDB**, processes and cleans the data, calculates **loan portfolio metrics**, and loads the transformed data into a **target PostgreSQL database (portfolio DB)** for further analysis and reporting.  

The pipeline simulates how **banks and NBFCs** manage customer loan portfolios by combining **customer, loan, and loan extension data**, applying **business rules** (arrears, EMI demand, DPD buckets, etc.), and generating both **aggregate loan metrics** and a **month-wise repayment schedule**.  

---

## ⚙️ Tech Stack  
- 🐍 **Python** (ETL logic, data cleaning, transformations)  
- 🛢️ **PostgreSQL** (Source: `etl` DB, Target: `portfolio` DB)  
- 🍃 **MongoDB** (Loan extensions data)  
- 📊 **pandas** (Data manipulation)  
- 🔗 **SQLAlchemy + psycopg2** (Postgres connections)  
- 🧹 **rapidfuzz** (Fuzzy string matching for region cleaning)  
- 🔑 **dotenv** (Environment variable management)  

---

###🚀 Key Highlights

✅ Realistic loan portfolio management ETL use case
✅ Handles multi-source integration (PostgreSQL + MongoDB)
✅ Implements fuzzy matching to clean inconsistent region names
✅ Computes financial metrics (arrears, EMI, outstanding, DPD buckets)
✅ Generates month-wise repayment schedule for portfolio monitoring
✅ Outputs analytics-ready tables in target DB


## 🏗️ Architecture  

### 🔹 Extract  
- **Source PostgreSQL (etl DB)**  
  - Schema: `customer_mgmt.customers`  
  - Schema: `loan_mgmt.loans`  
- **MongoDB (loan_etl DB)**  
  - Collection: `loan_extensions`  

### 🔹 Transform  
- Join customer and loan data  
- Merge MongoDB loan extensions  
- Clean region column with **fuzzy matching** (`Bangalore, Mumbai, Delhi, Kolkata`)  
- Calculate portfolio metrics:  
  - EMI  
  - Total Demand till today  
  - Arrear  
  - Target Amount  
  - Amount to be Returned  
  - Months Due  
  - DPD (Days Past Due) & DPD Buckets (`Current, 1–30, 31–60, 61–90, 90+`)  
  - Final Outstanding  
- Generate **month-wise repayment schedule** for each loan  

### 🔹 Load  
- **Target PostgreSQL (portfolio DB, schema `test`)**  
  - `loan_final` → Aggregate loan metrics per loan  
  - `loan_monthly_schedule` → Month-wise EMI schedule  

---

## 📂 Project Structure  

├── .env # Source DB credentials (etl DB)
├── .env_target # Target DB credentials (portfolio DB)
├── connect_postgres.py # PostgreSQL connection (source)
├── connect_postgres_target.py # PostgreSQL connection (target)
├── connect_mongodb.py # MongoDB connection
├── main.py # ETL pipeline script


---

## ▶️ How to Run  

### 1️⃣ Setup Databases  
- Create PostgreSQL databases:  
  - `etl` → Source DB (with schemas `customer_mgmt` and `loan_mgmt`)  
  - `portfolio` → Target DB  
- Start MongoDB and create DB `loan_etl` with collection `loan_extensions`  

### 2️⃣ Set Environment Variables  
- Update `.env` → for source PostgreSQL (`etl` DB)  
- Update `.env_target` → for target PostgreSQL (`portfolio` DB)  

### 3️⃣ Install Dependencies  
```bash
pip install -r requirements.txt
---
###4️⃣ Run ETL Pipeline
python main.py






