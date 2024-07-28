import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime

# Function to read and process data
def process_data(result_sql_file, card_file, phase1_file, phase2_self_pay_file, phase2_not_pay_file):
    result_sql = pd.read_csv(result_sql_file)
    card = pd.read_csv(card_file)

    hopefull_card = pd.read_excel(phase1_file)
    phase2_self_pay_card = pd.read_excel(phase2_self_pay_file)
    phase2_not_pay_card = pd.read_excel(phase2_not_pay_file)

    card["installment_uniqueid"] = card["installment_uniqueid"].astype(str)
    hopefull_card["installment_uniqueid"] = hopefull_card["installment_uniqueid"].astype(str)
    phase2_self_pay_card["installment_uniqueid"] = phase2_self_pay_card["installment_uniqueid"].astype(str)
    phase2_not_pay_card["installment_uniqueid"] = phase2_not_pay_card["installment_uniqueid"].astype(str)

    our_cases_card = card.merge(
        hopefull_card[["installment_uniqueid"]], on="installment_uniqueid", how="inner"
    )
    our_cases_card['trx_actual_collection_date'] = pd.to_datetime(our_cases_card['trx_actual_collection_date'])
    our_cases_card['trx_actual_collection_date_only'] = our_cases_card['trx_actual_collection_date'].dt.date

    collected_cases_card = our_cases_card[our_cases_card["status"] == "Collected"]
    ids_with_dates_card = collected_cases_card[["installment_uniqueid", "trx_actual_collection_date"]].drop_duplicates()

    result_sql["start_working_date"] = pd.to_datetime(result_sql["start_working_date"])
    result_sql["installment_uniqueid"] = result_sql["installment_uniqueid"].astype(str)

    start_date = pd.Timestamp("2024-07-05")
    filtered_result_sql = result_sql[result_sql["start_working_date"] >= start_date]
    check_card = filtered_result_sql.merge(ids_with_dates_card, on="installment_uniqueid", how="inner")

    test_phase2_card = card.copy()
    our_cases_2_card = test_phase2_card.merge(
        phase2_self_pay_card[["installment_uniqueid"]], on="installment_uniqueid", how="inner"
    )
    our_cases_2_card['trx_actual_collection_date'] = pd.to_datetime(our_cases_2_card['trx_actual_collection_date'])
    our_cases_2_card['trx_actual_collection_date_only'] = our_cases_2_card['trx_actual_collection_date'].dt.date

    collected_cases_2_card = our_cases_2_card[our_cases_2_card["status"] == "Collected"]
    ids_with_dates2_card = collected_cases_2_card[["installment_uniqueid", "trx_actual_collection_date"]].drop_duplicates()
    check2_card = filtered_result_sql.merge(ids_with_dates2_card, on="installment_uniqueid", how="inner")

    our_cases_3_card = test_phase2_card.merge(
        phase2_not_pay_card[["installment_uniqueid"]], on="installment_uniqueid", how="inner"
    )
    our_cases_3_card['trx_actual_collection_date'] = pd.to_datetime(our_cases_3_card['trx_actual_collection_date'])
    our_cases_3_card['trx_actual_collection_date_only'] = our_cases_3_card['trx_actual_collection_date'].dt.date

    collected_cases_3_card = our_cases_3_card[our_cases_3_card["status"] == "Collected"]
    ids_with_dates3_card = collected_cases_3_card[["installment_uniqueid", "trx_actual_collection_date"]].drop_duplicates()
    check3_card = filtered_result_sql.merge(ids_with_dates3_card, on="installment_uniqueid", how="inner")

    return {
        "Phase 1 Collection Rate": collected_cases_card["installment_uniqueid"].nunique() / hopefull_card["installment_uniqueid"].nunique(),
        "Phase 1 Count": collected_cases_card["installment_uniqueid"].nunique(),
        "Phase 1 Rate with Working Date": check_card[check_card["start_working_date"] < check_card["trx_actual_collection_date"]].installment_uniqueid.nunique() / hopefull_card.shape[0],
        "Phase 1 Count with Working Date": check_card[check_card["start_working_date"] < check_card["trx_actual_collection_date"]].installment_uniqueid.nunique(),
        "Phase 2 Collection Rate (Self Pay)": collected_cases_2_card["installment_uniqueid"].nunique() / phase2_self_pay_card["installment_uniqueid"].nunique(),
        "Phase 2 Count (Self Pay)": collected_cases_2_card["installment_uniqueid"].nunique(),
        "Phase 2 Rate with Working Date (Self Pay)": check2_card[check2_card["start_working_date"] < check2_card["trx_actual_collection_date"]].installment_uniqueid.nunique() / our_cases_2_card.shape[0],
        "Phase 2 Count with Working Date (Self Pay)": check2_card[check2_card["start_working_date"] < check2_card["trx_actual_collection_date"]].installment_uniqueid.nunique(),
        "Phase 2 Count (Not Pay)": collected_cases_3_card["installment_uniqueid"].nunique(),
        "Phase 2 Collection Rate (Not Pay)": collected_cases_3_card["installment_uniqueid"].nunique() / phase2_not_pay_card["installment_uniqueid"].nunique(),
        "Phase 2 Rate with Working Date (Not Pay)": check3_card[check3_card["start_working_date"] < check3_card["trx_actual_collection_date"]].installment_uniqueid.nunique() / our_cases_3_card.shape[0],
        "Phase 2 Count with Working Date (Not Pay)": check3_card[check3_card["start_working_date"] < check3_card["trx_actual_collection_date"]].installment_uniqueid.nunique()
    }

# Streamlit app
st.title("Collection Data Analysis")

result_sql_file = st.file_uploader("Upload result_sql.csv", type="csv")
card_file = st.file_uploader("Upload card_dues.csv", type="csv")
phase1_file = st.file_uploader("Upload Phase 1 Excel file", type="xlsx")
phase2_self_pay_file = st.file_uploader("Upload Phase 2 Self Pay Excel file", type="xlsx")
phase2_not_pay_file = st.file_uploader("Upload Phase 2 Not Pay Excel file", type="xlsx")

if result_sql_file and card_file and phase1_file and phase2_self_pay_file and phase2_not_pay_file:
    data = process_data(result_sql_file, card_file, phase1_file, phase2_self_pay_file, phase2_not_pay_file)

    st.write("### Collection Rates and Counts")
    df = pd.DataFrame.from_dict(data, orient='index', columns=['Value'])
    st.table(df)

# Dropdown menu for user input
category = st.selectbox("Select Category", ["Card", "Normal"])
phase = st.selectbox("Select Phase", ["Phase 1", "Phase 2"])

# Process data based on user input
if category == "Card":
    if phase == "Phase 1":
        df = process_data(result_sql_file, card_file, phase1_file, phase2_self_pay_file, phase2_not_pay_file)
    elif phase == "Phase 2":
        df = process_data(result_sql_file, card_file, phase1_file, phase2_self_pay_file, phase2_not_pay_file)
else:
    st.write("Normal category processing not implemented.")

# Data comparison between phases
if st.button('Compare Phases'):
    df_phase1 = process_data(result_sql_file, card_file, phase1_file, phase2_self_pay_file, phase2_not_pay_file)
    df_phase2 = process_data(result_sql_file, card_file, phase1_file, phase2_self_pay_file, phase2_not_pay_file)
    collected_phase1, collected_phase2 = compare_phases(df_phase1, df_phase2)
    st.write(f"Collected in Phase 1: {collected_phase1}")
    st.write(f"Collected in Phase 2: {collected_phase2}")

st.write("Upload your data files here:")
uploaded_file = st.file_uploader("Choose a file", type=["csv", "xlsx"])
if uploaded_file is not None:
    if uploaded_file.name.endswith('.csv'):
        df_uploaded = pd.read_csv(uploaded_file)
    elif uploaded_file.name.endswith('.xlsx'):
        df_uploaded = pd.read_excel(uploaded_file)
    st.write(df_uploaded)

if __name__ == "__main__":
    main()
