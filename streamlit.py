import streamlit as st
import pandas as pd
from datetime import datetime

# Function to read and process data
def process_data(result_sql_file, card_file, phase1_file, phase2_self_pay_file, phase2_not_pay_file):
    try:
        result_sql = pd.read_csv(result_sql_file)
    except Exception as e:
        st.error(f"Error reading result_sql.csv: {e}")
        return None

    try:
        card = pd.read_csv(card_file)
    except Exception as e:
        st.error(f"Error reading card_dues.csv: {e}")
        return None

    try:
        hopefull_card = pd.read_excel(phase1_file)
    except Exception as e:
        st.error(f"Error reading Phase 1 Excel file: {e}")
        return None

    try:
        phase2_self_pay_card = pd.read_excel(phase2_self_pay_file)
    except Exception as e:
        st.error(f"Error reading Phase 2 Self Pay Excel file: {e}")
        return None

    try:
        phase2_not_pay_card = pd.read_excel(phase2_not_pay_file)
    except Exception as e:
        st.error(f"Error reading Phase 2 Not Pay Excel file: {e}")
        return None

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

st.sidebar.title("Upload Files")
result_sql_file = st.sidebar.file_uploader("Upload result_sql.csv", type="csv")
card_file = st.sidebar.file_uploader("Upload card_dues.csv", type="csv")
phase1_file = st.sidebar.file_uploader("Upload Phase 1 Excel file", type="xlsx")
phase2_self_pay_file = st.sidebar.file_uploader("Upload Phase 2 Self Pay Excel file", type="xlsx")
phase2_not_pay_file = st.sidebar.file_uploader("Upload Phase 2 Not Pay Excel file", type="xlsx")

if result_sql_file and card_file and phase1_file and phase2_self_pay_file and phase2_not_pay_file:
    data = process_data(result_sql_file, card_file, phase1_file, phase2_self_pay_file, phase2_not_pay_file)

    if data is not None:
        st.write("### Collection Rates and Counts")
        df = pd.DataFrame({
            "Metrics": ["Collection Rate", "Count", "After Call Rate", "After Call Count"],
            "Phase 1": [
                data["Phase 1 Collection Rate"],
                data["Phase 1 Count"],
                data["Phase 1 Rate with Working Date"],
                data["Phase 1 Count with Working Date"]
            ],
            "Phase 2 (Self Pay)": [
                data["Phase 2 Collection Rate (Self Pay)"],
                data["Phase 2 Count (Self Pay)"],
                data["Phase 2 Rate with Working Date (Self Pay)"],
                data["Phase 2 Count with Working Date (Self Pay)"]
            ],
            "Phase 2 (Not Pay)": [
                data["Phase 2 Collection Rate (Not Pay)"],
                data["Phase 2 Count (Not Pay)"],
                data["Phase 2 Rate with Working Date (Not Pay)"],
                data["Phase 2 Count with Working Date (Not Pay)"]
            ]
        })
        st.table(df)
