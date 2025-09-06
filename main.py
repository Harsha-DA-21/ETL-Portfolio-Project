# main.py
import pandas as pd
from connect_postgres import get_connection          # Source (etl DB)
from connect_mongodb import get_mongo_connection     # MongoDB
from connect_postgres_target import get_target_engine  # Target (portfolio DB)
from rapidfuzz import process
from datetime import datetime
from sqlalchemy import text

try:
    # =========================
    # 🔹 Connect to Source PostgreSQL (etl DB)
    # =========================
    conn = get_connection()
    print("✅ Connected to Source PostgreSQL (etl DB)")
    with conn.cursor() as cur:
        cur.execute("SELECT current_database();")
        print("📌 Source DB in use:", cur.fetchone()[0])

    schema_customers = "customer_mgmt"
    schema_loan = "loan_mgmt"
    customers = "customers"
    loans = "loans"

    query_join = f"""
    SELECT 
        c.customer_id,
        c.customer_name,
        l.loan_id,
        l.disbursement_amount,
        l.disbursement_date,
        l.due_date,
        l.region,
        l.interest_rate,
        l.pos
    FROM {schema_customers}.{customers} c
    JOIN {schema_loan}.{loans} l
        ON c.loan_id = l.loan_id;
    """

    df_joined = pd.read_sql_query(query_join, conn)
    print("\n📊 Combined Data (customers + loans):")
    print(df_joined.head())
    conn.close()

    # =========================
    # 🔹 Connect to MongoDB
    # =========================
    mongo_db = get_mongo_connection()
    print("✅ Connected to MongoDB")
    loan_extension = mongo_db["loan_extensions"]
    loan_extension_data = list(
        loan_extension.find({}, {"_id": 0, "loan_id": 1, "product_type": 1, "emi_collected": 1})
    )
    df_extension = pd.DataFrame(loan_extension_data)
    print("\n📊 Loan Extensions Data (from MongoDB):")
    print(df_extension.head())

    # =========================
    # 🔹 Merge Postgres + Mongo Data
    # =========================
    final_df = df_joined.merge(df_extension, on="loan_id", how="left")

    # =========================
    # 🔹 Clean Region (Fuzzy Matching)
    # =========================
    valid_cities = ["Bangalore", "Mumbai", "Delhi", "Kolkata"]

    def clean_region(region):
        if pd.isna(region):
            return region
        best_match, score, _ = process.extractOne(region, valid_cities)
        return best_match

    final_df["region_cleaned"] = final_df["region"].apply(clean_region)

    # =========================
    # 🔹 Aggregate Loan Metrics (loan_final)
    # =========================
    today = datetime.today()

    def calculate_loan_metrics(row):
        P = row["disbursement_amount"]
        R = row["interest_rate"] / 100
        disbursement_date = pd.to_datetime(row["disbursement_date"])
        due_date = pd.to_datetime(row["due_date"])
        emi_collected = row.get("emi_collected", 0) or 0

        # Duration in months
        duration_months = (due_date.year - disbursement_date.year) * 12 + (due_date.month - disbursement_date.month)
        T = duration_months / 12

        # Interest and total payable
        interest = P * R * T
        total_payable = P + interest
        emi = total_payable / duration_months if duration_months > 0 else 0

        # Months due till today
        months_due = (today.year - disbursement_date.year) * 12 + (today.month - disbursement_date.month)
        months_due = max(0, min(months_due, duration_months))

        # Total demand till today
        total_demand = emi * months_due

        # Total amount still to be collected (Final outstanding)
        total_to_be_collected = total_payable - emi_collected 

        

        # Arrear
        arrear = max(0, total_demand - emi_collected)

        # Target & amount to be returned
        target_amount = max(0, arrear + emi - emi_collected)
        amount_to_be_returned = max(0, -1 * (arrear + emi - emi_collected))

        # DPD days
        emi_due_date = disbursement_date + pd.DateOffset(months=months_due) if arrear > 0 else disbursement_date
        dpd_days = max(0, (today - emi_due_date).days) if arrear > 0 else 0

        # DPD Bucket
        if dpd_days == 0:
            dpd_bucket = "Current"
        elif dpd_days <= 30:
            dpd_bucket = "1–30 DPD"
        elif dpd_days <= 60:
            dpd_bucket = "31–60 DPD"
        elif dpd_days <= 90:
            dpd_bucket = "61–90 DPD"
        else:
            dpd_bucket = "90+ DPD"

        return pd.Series([
            emi, total_demand, arrear, target_amount, amount_to_be_returned,
            months_due, dpd_days, dpd_bucket,total_to_be_collected
        ])

    final_df[[
        "emi_amount", "total_demand", "arrear", "target_amount", "amount_to_be_returned",
        "months_due", "dpd_days", "dpd_bucket","total_to_be_collected"
    ]] = final_df.apply(calculate_loan_metrics, axis=1)

    # =========================
    # 🔹 Month-wise Loan Schedule
    # =========================
    def generate_monthly_schedule(row):
        schedule = []
        P = row["disbursement_amount"]
        R = row["interest_rate"] / 100
        disbursement_date = pd.to_datetime(row["disbursement_date"])
        due_date = pd.to_datetime(row["due_date"])
        emi_collected = row.get("emi_collected", 0) or 0

        duration_months = (due_date.year - disbursement_date.year) * 12 + (due_date.month - disbursement_date.month)
        total_payable = P * (1 + R * (duration_months / 12))
        emi_amount = total_payable / duration_months if duration_months > 0 else 0

        total_collected = 0
        for m in range(1, duration_months + 1):
            emi_due_date = disbursement_date + pd.DateOffset(months=m)
            total_demand_till_month = emi_amount * m
            collected = emi_collected if emi_due_date <= today else 0
            total_collected += collected
            arrear = max(0, total_demand_till_month - total_collected)
            dpd_days = max(0, (today - emi_due_date).days) if arrear > 0 else 0

            if dpd_days == 0:
                dpd_bucket = "Current"
            elif dpd_days <= 30:
                dpd_bucket = "0–30"
            elif dpd_days <= 60:
                dpd_bucket = "30–60"
            elif dpd_days <= 90:
                dpd_bucket = "60–90"
            else:
                dpd_bucket = "90+"

            schedule.append({
                "loan_id": row["loan_id"],
                "Month": m,
                "EMI Due Date": emi_due_date.strftime("%d-%b-%Y"),
                "EMI Amount": round(emi_amount, 2),
                "EMI Collected": round(collected, 2),
                "Total Demand Till Month": round(total_demand_till_month, 2),
                "Arrear": round(arrear, 2),
                "DPD (as of {})".format(today.strftime("%d-%b-%Y")): dpd_days,
                "DPD Bucket": dpd_bucket
            })
        return pd.DataFrame(schedule)

    # Generate month-wise schedule for all loans
    schedule_list = [generate_monthly_schedule(row) for _, row in final_df.iterrows()]
    monthly_schedule_df = pd.concat(schedule_list, ignore_index=True)

    # =========================
    # 🔹 Load to Target DB (portfolio)
    # =========================
    target_engine = get_target_engine()
    with target_engine.connect() as conn:
        db_name = conn.execute(text("SELECT current_database();")).scalar()
        print("📌 Target DB in use:", db_name)

    # Aggregate metrics table
    final_df.to_sql(
        "loan_final",
        con=target_engine,
        schema="test",
        if_exists="replace",
        index=False
    )

    # Month-wise schedule table
    monthly_schedule_df.to_sql(
        "loan_monthly_schedule",
        con=target_engine,
        schema="test",
        if_exists="replace",
        index=False
    )

    print("🎯 Data successfully transferred to target DB (aggregate + monthly schedule)")

except Exception as e:
    print("❌ Error:", e)




























































































# # main.py
# import pandas as pd
# from connect_postgres import get_connection          # Source (etl DB)
# from connect_mongodb import get_mongo_connection     # MongoDB
# from connect_postgres_target import get_target_engine  # Target (portfolio DB)
# from rapidfuzz import process
# from datetime import datetime
# from sqlalchemy import text

# try:
#     # =========================
#     # 🔹 Connect to Source PostgreSQL (etl DB)
#     # =========================


#     conn = get_connection()
#     print("✅ Connected to Source PostgreSQL (etl DB)")
#     with conn.cursor() as cur:
#         cur.execute("SELECT current_database();")
#         print("📌 Source DB in use:", cur.fetchone()[0])

#     # Schemas & tables in source
#     schema_customers = "customer_mgmt"
#     schema_loan = "loan_mgmt"
#     customers = "customers"
#     loans = "loans"

#     # Join customers + loans
#     query_join = f"""
#     SELECT 
#         c.customer_id,
#         c.customer_name,
#         l.loan_id,
#         l.disbursement_amount,
#         l.disbursement_date,
#         l.due_date,
#         l.region,
#         l.interest_rate,
#         l.pos
#     FROM {schema_customers}.{customers} c
#     JOIN {schema_loan}.{loans} l
#         ON c.loan_id = l.loan_id;
#     """

#     df_joined = pd.read_sql_query(query_join, conn)
#     print("\n📊 Combined Data (customers + loans):")
#     print(df_joined.head())

#     conn.close()   # ✅ Close source connection immediately


#     # =========================
#     # 🔹 Connect to MongoDB
#     # =========================


#     mongo_db = get_mongo_connection()
#     print("✅ Connected to MongoDB")

#     loan_extension = mongo_db["loan_extensions"]

#     loan_extension_data = list(
#         loan_extension.find({}, {"_id": 0, "loan_id": 1, "product_type": 1, "emi_collected": 1})
#     )

#     df_extension = pd.DataFrame(loan_extension_data)
#     print("\n📊 Loan Extensions Data (from MongoDB):")
#     print(df_extension.head())


#     # =========================
#     # 🔹 Merge Postgres + Mongo Data
#     # =========================

#     final_df = df_joined.merge(df_extension, on="loan_id", how="left")

    
#        #     So your flow is now:
#        # # Query Postgres → customer + loan details.
#        # # Query MongoDB → product_type + emi_collected.
#        # # Merge on loan_id with how="left".
#        # # Get one clean combined dataset ✅





#     # =========================
#     # 🔹 Clean Region (Fuzzy Matching)
#     # =========================


#     valid_cities = ["Bangalore", "Mumbai", "Delhi", "Kolkata"]

#     def clean_region(region):
#         if pd.isna(region):
#             return region
#         best_match, score, _ = process.extractOne(region, valid_cities)
#         return best_match

#     final_df["region_cleaned"] = final_df["region"].apply(clean_region)

#     # =========================
#     # 💰 Loan Metrics Calculation
#     # =========================


#     today = datetime.today()

#     def calculate_loan_metrics(row):
#         try:
#             P = row["disbursement_amount"]
#             R = row["interest_rate"] / 100
#             disbursement_date = pd.to_datetime(row["disbursement_date"])
#             due_date = pd.to_datetime(row["due_date"])

#             # Duration in months
#             duration_months = (due_date.year - disbursement_date.year) * 12 + \
#                               (due_date.month - disbursement_date.month)
#             T = duration_months / 12

#             # Interest and total payable
#             interest = P * R * T
#             total_payable = P + interest

#             # EMI
#             emi = total_payable / duration_months if duration_months > 0 else 0

#             # Demand
#             months_due = (today.year - disbursement_date.year) * 12 + \
#                          (today.month - disbursement_date.month)
#             months_due = max(0, min(months_due, duration_months))
#             total_demand = emi * months_due

#             # Arrear
#             collected = row.get("emi_collected", 0) or 0
#             arrear = max(0, total_demand - collected)

#             # Target
#             target_amount = arrear + emi - collected


#              # Handle negative target
#             if target_amount < 0:
#                 amount_to_be_returned = abs(target_amount)  #  # ✅ convert to positive
#                 target_amount = 0
#             else:
#              amount_to_be_returned = 0

#             # DPD
#             if arrear > 0:
#                 emi_due_date = disbursement_date + pd.DateOffset(months=months_due)
#                 dpd_days = max(0, (today - emi_due_date).days)
#             else:
#                 dpd_days = 0

#             if dpd_days == 0:
#                 dpd_bucket = "Current"
#             elif dpd_days <= 30:
#                 dpd_bucket = "1–30 DPD"
#             elif dpd_days <= 60:
#                 dpd_bucket = "31–60 DPD"
#             elif dpd_days <= 90:
#                 dpd_bucket = "61–90 DPD"
#             else:
#                 dpd_bucket = "90+ DPD"

#             return pd.Series([emi, total_demand, arrear, target_amount,amount_to_be_returned, dpd_days, dpd_bucket])

#         except Exception:
#             return pd.Series([None, None, None, None, None, None,None])

#     final_df[["emi_amount", "total_demand", "arrear", "target_amount","amount_to_be_returned",
#               "dpd_days", "dpd_bucket"]] = final_df.apply(calculate_loan_metrics, axis=1)

#     print("\n📊 Final Loan Data with Metrics:")
#     print(final_df.head())


#     # =========================
#     # 🔹 Load to Target DB (portfolio)
#     # =========================


#     target_engine = get_target_engine()
#     with target_engine.connect() as conn:
#         db_name = conn.execute(text("SELECT current_database();")).scalar()
#         print("📌 Target DB in use:", db_name)

#     final_df.to_sql(
#         "loan_final",
#         con=target_engine,
#         schema="test",
#         if_exists="replace",
#         index=False
#     )
#     print("🎯 Data successfully transferred to target DB (schema=test, table=loan_final)")


# except Exception as e:
#     print("❌ Error:", e)



































































































# import pandas as pd
# from connect_postgres import get_connection
# from connect_mongodb import  get_mongo_connection
# from rapidfuzz import process
# from datetime import datetime

# from connect_postgres_target import get_target_engine


# try:


#     # =========================
#     # 🔹 PostgreSQL Query
#     # =========================


#     conn = get_connection()
#     print(" ✅ Connected to PostgreSql")


#     # 1️⃣ Define schema

#     schema_customers = "customer_mgmt"
#     schema_loan      = "loan_mgmt"

#     # 2️⃣ Define Table

#     customers = "customers"
#     loans     = "loans"

   

#     # 3️⃣ Build JOIN query dynamically
#     query_join = f"""
#     SELECT 
#         c.customer_id,
#         c.customer_name,
#         l.loan_id,
#         l.disbursement_amount,
#         l.disbursement_date,
#         l.due_date,
#         l.region,
#         l.interest_rate,
#         l.pos
#     FROM {schema_customers}.{customers} c
#     JOIN {schema_loan}.{loans} l
#         ON c.loan_id = l.loan_id;
#     """

#     # 4️⃣ Run query
#     df_joined = pd.read_sql_query(query_join, conn)

#     print("\n📊 Combined Data (customers + loans):")
#     print(df_joined)




#     conn.close()



#     # =========================
#     # 🔹 MongoDB
#     # =========================

#     mongo_db = get_mongo_connection()
#     print("✅ Connected to MongoDB")

#     loan_extension=mongo_db["loan_extensions"]


#     # only fetch requird fields 

#     loan_extension_data = list(loan_extension.find({},{"_id":0,"loan_id":1,"product_type":1,"emi_collected":1}))

#     df_extension = pd.DataFrame(loan_extension_data)

#     print("\n📊 Loan Extensions Data (from MongoDB):")
#     print(df_extension)





#     conn.close()
   

#    #=========================================
#    # Merge Postgres + Mongo data on loan_id
#    #=========================================

#     final_df = df_joined.merge(df_extension,on="loan_id",how="left")
#     # print("\n📊 Final Combined Data (Postgres + Mongo):")
#     # print(final_df)

#        #     So your flow is now:
#        # # Query Postgres → customer + loan details.
#        # # Query MongoDB → product_type + emi_collected.
#        # # Merge on loan_id with how="left".
#        # # Get one clean combined dataset ✅


#        # 5️⃣ Select only specific columns for checking
#     # df_check = final_df[["customer_name", "region", "product_type"]]

#     # print("\n📊 Quick Check (customer_name, region, product_type):")
#     # print(df_check)



#     # =========================
#     # 🔹 Fuzzy String Matching on Region
#     # =========================



#     valid_cities = ["Bangalore", "Mumbai", "Delhi", "Kolkata"]

#     def clean_region(region):
#         if pd.isna(region):
#             return region
#         best_match, score, _ = process.extractOne(region, valid_cities)
#         return best_match

#     final_df["region_cleaned"] = final_df["region"].apply(clean_region)

#     # Quick check

#     # df_check = final_df[["customer_name", "region", "region_cleaned", "product_type"]]
#     # print("\n📊 Quick Check (customer_name, region → region_cleaned, product_type):")
#     # print(df_check)


#     # ============================
#     # 💰 EMI, Demand, Arrear Calc
#     # ============================

#     today = datetime.today()

#     def calculate_loan_metrics(row):
#       try:
#          P = row["disbursement_amount"]  # Principal
#          R = row["interest_rate"] / 100  # Rate (per year)
#          disbursement_date = pd.to_datetime(row["disbursement_date"])
#          due_date = pd.to_datetime(row["due_date"])

#         # Loan duration in months
#          duration_months = (due_date.year - disbursement_date.year) * 12 + (due_date.month - disbursement_date.month)
#          T = duration_months / 12  # in years

#         # Total interest & total payable
#          interest = P * R * T
#          total_payable = P + interest

#         # EMI per month
#          emi = total_payable / duration_months if duration_months > 0 else 0

#         # EMIs due till today
#          months_due = (today.year - disbursement_date.year) * 12 + (today.month - disbursement_date.month)
#          months_due = max(0, min(months_due, duration_months))  # cap between 0 and total duration

#          total_demand = emi * months_due

#         # Arrear = demand - collected
#          collected = row.get("emi_collected", 0) or 0
#          arrear = max(0, total_demand - collected)


#          # ✅ Target amount
#          target_amount = arrear + emi - collected



#           # ======================
#          # 🔹 DPD Calculation
#          # ======================
#          if arrear > 0:
#             # Oldest unpaid EMI due date
#             emi_due_date = disbursement_date + pd.DateOffset(months=months_due)
#             dpd_days = max(0, (today - emi_due_date).days)
#          else:
#             dpd_days = 0

#          # Bucket assignment
#          if dpd_days == 0:
#             dpd_bucket = "Current"
#          elif dpd_days <= 30:
#             dpd_bucket = "1–30 DPD"
#          elif dpd_days <= 60:
#             dpd_bucket = "31–60 DPD"
#          elif dpd_days <= 90:
#             dpd_bucket = "61–90 DPD"
#          else:
#             dpd_bucket = "90+ DPD"







    

#          return pd.Series([emi, total_demand, arrear,target_amount,dpd_days, dpd_bucket])
#       except Exception as e:
#         return pd.Series([None, None, None,None,None,None])

# # Apply function
#     final_df[["emi_amount", "total_demand", "arrear","target_amount","dpd_days", "dpd_bucket"]] = final_df.apply(calculate_loan_metrics, axis=1)

# # Show final results
#     print("\n📊 Final Loan Data with EMI, Total Demand, and Arrear:")
#     print(final_df[["customer_name", "region_cleaned", "product_type", "emi_amount", "total_demand", "arrear","target_amount","emi_collected","dpd_days", "dpd_bucket"]])
#     print("\n📋 All Columns in final_df:")
#     print(final_df.columns.tolist())

#     # =========================
#     # 🔹 Load to Target DB
#     # =========================
#     target_engine = get_target_engine()
#     print("✅ Connected to Target PostgreSQL")

#     final_df.to_sql("loan_final", con=target_engine, schema="test", if_exists="replace", index=False)
#     print("🎯 Data successfully transferred to target DB (schema=test, table=loan_final)")



#     # =========================
#     # 🔹 Preview
#     # =========================
#     print("\n📊 Final Data Sample:")
#     print(final_df.head())







# except Exception as e:
#     print("Error :",e)



