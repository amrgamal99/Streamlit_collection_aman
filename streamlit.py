import pandas as pd
import streamlit as st
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize findspark
findspark.init()

# Create or get Spark session
try:
    spark = SparkSession.builder.getOrCreate()
except Exception as e:
    st.error(f"Error initializing Spark session: {e}")

# Define file paths (adjust paths as needed)
result_sql_path = "C:/Users/PC/OneDrive - Cairo University - Students/aman internship/collection_data/Data Science - Internship-20240723T123248Z-001/Data Science - Internship/data/result_sql.csv"
card_path = "C:/Users/PC/OneDrive - Cairo University - Students/aman internship/collection_data/Data Science - Internship-20240723T123248Z-001/Data Science - Internship/data/card_dues.csv"
normal_path = "C:/Users/PC/OneDrive - Cairo University - Students/aman internship/collection_data/Data Science - Internship-20240723T123248Z-001/Data Science - Internship/data/normal_dues.csv"

# Load data
try:
    result_sql = pd.read_csv(result_sql_path)
    result_sql_spark = spark.createDataFrame(result_sql)
    card = pd.read_csv(card_path)
    normal = pd.read_csv(normal_path)
    date = "2024-07"
    next_date = "2024-08"
except FileNotFoundError as e:
    st.error(f"File not found: {e}")
except Exception as e:
    st.error(f"Error loading data: {e}")

# Define function to process data
def process_data(phase1_file, phase2_paid_file, phase2_not_paid_file, dues_data, result_sql_spark, date):
    try:
        # Phase 1
        phase1_data = dues_data.copy()
        phase1_data["installment_uniqueid"] = phase1_data["installment_uniqueid"].astype(str)
        phase1_customers = pd.read_excel(phase1_file)
        phase1_customers["installment_uniqueid"] = phase1_customers["installment_uniqueid"].astype(str)
        phase1_cases = phase1_data.merge(phase1_customers[["installment_uniqueid"]], on="installment_uniqueid", how="inner")
        phase1_cases['trx_actual_collection_date'] = pd.to_datetime(phase1_cases['trx_actual_collection_date'])
        phase1_cases['trx_actual_collection_date_only'] = phase1_cases['trx_actual_collection_date'].dt.date.astype(str)
        
        # Metrics for phase 1
        total_customers_phase1 = phase1_customers["installment_uniqueid"].nunique()
        paid_customers_phase1 = phase1_cases[phase1_cases["status"] == "Collected"]["installment_uniqueid"].nunique()
        percentage_paid_phase1 = paid_customers_phase1 / total_customers_phase1 * 100
        paid_after_call_phase1 = phase1_cases[(phase1_cases["status"] == "Collected") & 
                                              (phase1_cases["trx_actual_collection_date"] >= date + "-05") &
                                              (phase1_cases["trx_actual_collection_date"] < next_date + "-05")]
        paid_after_call_phase1_count = paid_after_call_phase1["installment_uniqueid"].nunique()
        
        # Phase 2 Paid
        phase2_paid_data = dues_data.copy()
        phase2_paid_data["installment_uniqueid"] = phase2_paid_data["installment_uniqueid"].astype(str)
        phase2_paid_customers = pd.read_excel(phase2_paid_file)
        phase2_paid_customers["installment_uniqueid"] = phase2_paid_customers["installment_uniqueid"].astype(str)
        phase2_paid_cases = phase2_paid_data.merge(phase2_paid_customers[["installment_uniqueid"]], on="installment_uniqueid", how="inner")
        phase2_paid_cases['trx_actual_collection_date'] = pd.to_datetime(phase2_paid_cases['trx_actual_collection_date'])
        phase2_paid_cases['trx_actual_collection_date_only'] = phase2_paid_cases['trx_actual_collection_date'].dt.date.astype(str)
        
        # Metrics for phase 2 paid
        total_customers_phase2_paid = phase2_paid_customers["installment_uniqueid"].nunique()
        paid_customers_phase2_paid = phase2_paid_cases[phase2_paid_cases["status"] == "Collected"]["installment_uniqueid"].nunique()
        percentage_paid_phase2_paid = paid_customers_phase2_paid / total_customers_phase2_paid * 100
        paid_after_call_phase2_paid = phase2_paid_cases[(phase2_paid_cases["status"] == "Collected") & 
                                                        (phase2_paid_cases["trx_actual_collection_date"] >= date + "-05") &
                                                        (phase2_paid_cases["trx_actual_collection_date"] < next_date + "-05")]
        paid_after_call_phase2_paid_count = paid_after_call_phase2_paid["installment_uniqueid"].nunique()
        
        # Phase 2 Not Paid
        phase2_not_paid_data = dues_data.copy()
        phase2_not_paid_data["installment_uniqueid"] = phase2_not_paid_data["installment_uniqueid"].astype(str)
        phase2_not_paid_customers = pd.read_excel(phase2_not_paid_file)
        phase2_not_paid_customers["installment_uniqueid"] = phase2_not_paid_customers["installment_uniqueid"].astype(str)
        phase2_not_paid_cases = phase2_not_paid_data.merge(phase2_not_paid_customers[["installment_uniqueid"]], on="installment_uniqueid", how="inner")
        phase2_not_paid_cases['trx_actual_collection_date'] = pd.to_datetime(phase2_not_paid_cases['trx_actual_collection_date'])
        phase2_not_paid_cases['trx_actual_collection_date_only'] = phase2_not_paid_cases['trx_actual_collection_date'].dt.date.astype(str)
        
        # Metrics for phase 2 not paid
        total_customers_phase2_not_paid = phase2_not_paid_customers["installment_uniqueid"].nunique()
        not_paid_customers_phase2_not_paid = phase2_not_paid_cases[phase2_not_paid_cases["status"] != "Collected"]["installment_uniqueid"].nunique()
        percentage_not_paid_phase2_not_paid = not_paid_customers_phase2_not_paid / total_customers_phase2_not_paid * 100
        not_paid_after_call_phase2_not_paid = phase2_not_paid_cases[(phase2_not_paid_cases["status"] != "Collected") & 
                                                                    (phase2_not_paid_cases["trx_actual_collection_date"] >= date + "-05") &
                                                                    (phase2_not_paid_cases["trx_actual_collection_date"] < next_date + "-05")]
        not_paid_after_call_phase2_not_paid_count = not_paid_after_call_phase2_not_paid["installment_uniqueid"].nunique()
        
        return {
            'phase1': {
                'total_customers': total_customers_phase1,
                'paid_customers': paid_customers_phase1,
                'percentage_paid': percentage_paid_phase1,
                'paid_after_call': paid_after_call_phase1_count
            },
            'phase2_paid': {
                'total_customers': total_customers_phase2_paid,
                'paid_customers': paid_customers_phase2_paid,
                'percentage_paid': percentage_paid_phase2_paid,
                'paid_after_call': paid_after_call_phase2_paid_count
            },
            'phase2_not_paid': {
                'total_customers': total_customers_phase2_not_paid,
                'not_paid_customers': not_paid_customers_phase2_not_paid,
                'percentage_not_paid': percentage_not_paid_phase2_not_paid,
                'not_paid_after_call': not_paid_after_call_phase2_not_paid_count
            }
        }
    except Exception as e:
        st.error(f"Error processing data: {e}")
        return {}

# Process data for card and normal customers
card_metrics = process_data(
    "C:/Users/PC/OneDrive - Cairo University - Students/aman internship/collection_data/Data Science
