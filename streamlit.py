import streamlit as st
import pandas as pd
from datetime import datetime

# Set display options
pd.set_option('display.max_rows', 500)
pd.options.display.max_columns = None

# Read the input files
result_sql = pd.read_csv("C:/Users/PC/OneDrive - Cairo University - Students/aman internship/collection_data/Data Science - Internship-20240723T123248Z-001/Data Science - Internship/data/result_sql.csv")
card = pd.read_csv("C:/Users/PC/OneDrive - Cairo University - Students/aman internship/collection_data/Data Science - Internship-20240723T123248Z-001/Data Science - Internship/data/card_dues.csv")

# Date variables
date = "2024-07"
next_date = "2024-08"

# Phase 1 Card
test_phase1_card = card.copy()
test_phase1_card["installment_uniqueid"] = test_phase1_card["installment_uniqueid"].astype(str)

hopefull_card = pd.read_excel("C:/Users/PC/OneDrive - Cairo University - Students/aman internship/collection_data/Data Science - Internship-20240723T123248Z-001/Data Science - Internship/list_of_customers/Card_Solution1_Will Pay Alone_2024-07.xlsx")
hopefull_card["installment_uniqueid"] = hopefull_card["installment_uniqueid"].astype(str)

our_cases_card = test_phase1_card.merge(hopefull_card[["installment_uniqueid"]], on="installment_uniqueid", how="inner")
our_cases_card['trx_actual_collection_date'] = pd.to_datetime(our_cases_card['trx_actual_collection_date'])
our_cases_card['trx_actual_collection_date_only'] = our_cases_card['trx_actual_collection_date'].dt.date.astype(str)

collected_ratio_phase1 = our_cases_card[our_cases_card["status"] == "Collected"].installment_uniqueid.nunique() / hopefull_card.installment_uniqueid.nunique()
collected_uniqueid_phase1 = our_cases_card[our_cases_card["status"] == "Collected"].installment_uniqueid.nunique()

ids_phase1_card = our_cases_card[our_cases_card["status"] == "Collected"]
ids_with_dates_card = ids_phase1_card[["installment_uniqueid", "trx_actual_collection_date"]].drop_duplicates()
ids_with_dates_card["trx_actual_collection_date"] = pd.to_datetime(ids_with_dates_card["trx_actual_collection_date"])

result_sql["start_working_date"] = pd.to_datetime(result_sql["start_working_date"])
result_sql["installment_uniqueid"] = result_sql["installment_uniqueid"].astype(str)
ids_with_dates_card["installment_uniqueid"] = ids_with_dates_card["installment_uniqueid"].astype(str)

start_date = pd.to_datetime(date + "-05")
filtered_result_sql = result_sql[result_sql["start_working_date"] >= start_date]

check_card = filtered_result_sql.merge(ids_with_dates_card, on="installment_uniqueid", how="inner")
check_collected_ratio_phase1 = check_card[check_card["start_working_date"] < check_card["trx_actual_collection_date"]].installment_uniqueid.nunique() / hopefull_card.shape[0]
check_collected_uniqueid_phase1 = check_card[check_card["start_working_date"] < check_card["trx_actual_collection_date"]].installment_uniqueid.nunique()

# Phase 2 Card - Will Pay Alone
phase2_self_pay_card = pd.read_excel("C:/Users/PC/OneDrive - Cairo University - Students/aman internship/collection_data/Data Science - Internship-20240723T123248Z-001/Data Science - Internship/list_of_customers/Card_Solution2_ Will Pay Alone_2024-07.xlsx")
phase2_self_pay_card["installment_uniqueid"] = phase2_self_pay_card["installment_uniqueid"].astype(str)

our_cases_2_card = test_phase2_card.merge(phase2_self_pay_card[["installment_uniqueid"]], on="installment_uniqueid", how="inner")
our_cases_2_card['trx_actual_collection_date'] = pd.to_datetime(our_cases_2_card['trx_actual_collection_date'])
our_cases_2_card['trx_actual_collection_date_only'] = our_cases_2_card['trx_actual_collection_date'].dt.date.astype(str)

collected_ratio_phase2_self = our_cases_2_card[our_cases_2_card["status"] == "Collected"].installment_uniqueid.nunique() / our_cases_2_card.installment_uniqueid.nunique()
collected_uniqueid_phase2_self = our_cases_2_card[our_cases_2_card["status"] == "Collected"].installment_uniqueid.nunique()

ids_phase2_card = our_cases_2_card[our_cases_2_card["status"] == "Collected"]
ids_with_dates2_card = ids_phase2_card[["installment_uniqueid", "trx_actual_collection_date"]].drop_duplicates()

filtered_result_sql = result_sql[result_sql["start_working_date"] >= start_date]
check2_card = filtered_result_sql.merge(ids_with_dates2_card, on="installment_uniqueid", how="inner")
check_collected_ratio_phase2_self = check2_card[check2_card["start_working_date"] < check2_card["trx_actual_collection_date"]].installment_uniqueid.nunique() / our_cases_2_card.shape[0]
check_collected_uniqueid_phase2_self = check2_card[check2_card["start_working_date"] < check2_card["trx_actual_collection_date"]].installment_uniqueid.nunique()

# Phase 2 Card - Will Not Pay
phase2_not_pay_card = pd.read_excel("C:/Users/PC/OneDrive - Cairo University - Students/aman internship/collection_data/Data Science - Internship-20240723T123248Z-001/Data Science - Internship/list_of_customers/Card_Solution2_ Will Not Pay_2024-07.xlsx")
phase2_not_pay_card["installment_uniqueid"] = phase2_not_pay_card["installment_uniqueid"].astype(str)

our_cases_3_card = test_phase2_card.merge(phase2_not_pay_card[["installment_uniqueid"]], on="installment_uniqueid", how="inner")
our_cases_3_card['trx_actual_collection_date'] = pd.to_datetime(our_cases_3_card['trx_actual_collection_date'])
our_cases_3_card['trx_actual_collection_date_only'] = our_cases_3_card['trx_actual_collection_date'].dt.date.astype(str)

collected_uniqueid_phase2_not = our_cases_3_card[our_cases_3_card["status"] == "Collected"].installment_uniqueid.nunique()
collected_ratio_phase2_not = collected_uniqueid_phase2_not / our_cases_3_card.installment_uniqueid.nunique()

ids_phase3_card = our_cases_3_card[our_cases_3_card["status"] == "Collected"][["installment_uniqueid", "trx_actual_collection_date"]].drop_duplicates()
ids_with_dates3_card = pd.DataFrame(ids_phase3_card)

filtered_result_sql = result_sql[result_sql['start_working_date'] >= start_date]
check3_card = filtered_result_sql.merge(ids_with_dates3_card, on="installment_uniqueid", how="inner")
check_collected_ratio_phase2_not = check3_card[check3_card["start_working_date"] < check3_card["trx_actual_collection_date"]].installment_uniqueid.nunique() / our_cases_3_card.shape[0]
check_collected_uniqueid_phase2_not = check3_card[check3_card["start_working_date"] < check3_card["trx_actual_collection_date"]].installment_uniqueid.nunique()

# Streamlit App
st.title('Collection Data Analysis')

# Display collected ratios and unique ids in a table
st.subheader('Phase 1 Card - Will Pay Alone')
st.write(pd.DataFrame({
    'Metric': ['Collected Ratio', 'Collected Unique IDs'],
    'Value': [collected_ratio_phase1, collected_uniqueid_phase1]
}))

st.subheader('Phase 2 Card - Will Pay Alone')
st.write(pd.DataFrame({
    'Metric': ['Collected Ratio', 'Collected Unique IDs'],
    'Value': [collected_ratio_phase2_self, collected_uniqueid_phase2_self]
}))

st.subheader('Phase 2 Card - Will Not Pay')
st.write(pd.DataFrame({
    'Metric': ['Collected Ratio', 'Collected Unique IDs'],
    'Value': [collected_ratio_phase2_not, collected_uniqueid_phase2_not]
}))

st.subheader('Check Card - Phase 1')
st.write(pd.DataFrame({
    'Metric': ['Collected Ratio', 'Collected Unique IDs'],
    'Value': [check_collected_ratio_phase1, check_collected_uniqueid_phase1]
}))

st.subheader('Check Card - Phase 2 (Will Pay Alone)')
st.write(pd.DataFrame({
    'Metric': ['Collected Ratio', 'Collected Unique IDs'],
    'Value': [check_collected_ratio_phase2_self, check_collected_uniqueid_phase2_self]
}))

st.subheader('Check Card - Phase 2 (Will Not Pay)')
st.write(pd.DataFrame({
    'Metric': ['Collected Ratio', 'Collected Unique IDs'],
    'Value': [check_collected_ratio_phase2_not, check_collected_uniqueid_phase2_not]
}))
