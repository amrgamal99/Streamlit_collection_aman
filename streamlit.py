import streamlit as st
import pandas as pd
from datetime import datetime

# Function to read and process data
def process_data(category, result_sql_file, dues_file, phase1_file, phase2_self_pay_file, phase2_not_pay_file):
    try:
        result_sql = pd.read_csv(result_sql_file)
    except Exception as e:
        st.error(f"Error reading result_sql.csv: {e}")
        return None

    try:
        dues = pd.read_csv(dues_file)
    except Exception as e:
        st.error(f"Error reading dues.csv: {e}")
        return None

    try:
        hopefull = pd.read_excel(phase1_file)
    except Exception as e:
        st.error(f"Error reading Phase 1 Excel file: {e}")
        return None

    try:
        phase2_self_pay = pd.read_excel(phase2_self_pay_file)
    except Exception as e:
        st.error(f"Error reading Phase 2 Self Pay Excel file: {e}")
        return None

    try:
        phase2_not_pay = pd.read_excel(phase2_not_pay_file)
    except Exception as e:
        st.error(f"Error reading Phase 2 Not Pay Excel file: {e}")
        return None

    dues["installment_uniqueid"] = dues["installment_uniqueid"].astype(str)
    hopefull["installment_uniqueid"] = hopefull["installment_uniqueid"].astype(str)
    phase2_self_pay["installment_uniqueid"] = phase2_self_pay["installment_uniqueid"].astype(str)
    phase2_not_pay["installment_uniqueid"] = phase2_not_pay["installment_uniqueid"].astype(str)

    our_cases = dues.merge(
        hopefull[["installment_uniqueid"]], on="installment_uniqueid", how="inner"
    )
    our_cases['trx_actual_collection_date'] = pd.to_datetime(our_cases['trx_actual_collection_date'])
    our_cases['trx_actual_collection_date_only'] = our_cases['trx_actual_collection_date'].dt.date

    collected_cases = our_cases[our_cases["status"] == "Collected"]
    ids_with_dates = collected_cases[["installment_uniqueid", "trx_actual_collection_date"]].drop_duplicates()

    result_sql["start_working_date"] = pd.to_datetime(result_sql["start_working_date"])
    result_sql["installment_uniqueid"] = result_sql["installment_uniqueid"].astype(str)

    start_date = pd.Timestamp("2024-07-05")
    filtered_result_sql = result_sql[result_sql["start_working_date"] >= start_date]
    check = filtered_result_sql.merge(ids_with_dates, on="installment_uniqueid", how="inner")

    test_phase2 = dues.copy()
    our_cases_2 = test_phase2.merge(
        phase2_self_pay[["installment_uniqueid"]], on="installment_uniqueid", how="inner"
    )
    our_cases_2['trx_actual_collection_date'] = pd.to_datetime(our_cases_2['trx_actual_collection_date'])
    our_cases_2['trx_actual_collection_date_only'] = our_cases_2['trx_actual_collection_date'].dt.date

    collected_cases_2 = our_cases_2[our_cases_2["status"] == "Collected"]
    ids_with_dates2 = collected_cases_2[["installment_uniqueid", "trx_actual_collection_date"]].drop_duplicates()
    check2 = filtered_result_sql.merge(ids_with_dates2, on="installment_uniqueid", how="inner")

    our_cases_3 = test_phase2.merge(
        phase2_not_pay[["installment_uniqueid"]], on="installment_uniqueid", how="inner"
    )
    our_cases_3['trx_actual_collection_date'] = pd.to_datetime(our_cases_3['trx_actual_collection_date'])
    our_cases_3['trx_actual_collection_date_only'] = our_cases_3['trx_actual_collection_date'].dt.date

    collected_cases_3 = our_cases_3[our_cases_3["status"] == "Collected"]
    ids_with_dates3 = collected_cases_3[["installment_uniqueid", "trx_actual_collection_date"]].drop_duplicates()
    check3 = filtered_result_sql.merge(ids_with_dates3, on="installment_uniqueid", how="inner")

    return {
        "Phase 1 Collection Rate": collected_cases["installment_uniqueid"].nunique() / hopefull["installment_uniqueid"].nunique(),
        "Phase 1 Count": collected_cases["installment_uniqueid"].nunique(),
        "Phase 1 Rate with Working Date": check[check["start_working_date"] < check["trx_actual_collection_date"]].installment_uniqueid.nunique() / hopefull.shape[0],
        "Phase 1 Count with Working Date": check[check["start_working_date"] < check["trx_actual_collection_date"]].installment_uniqueid.nunique(),
        "Phase 2 Collection Rate (Self Pay)": collected_cases_2["installment_uniqueid"].nunique() / phase2_self_pay["installment_uniqueid"].nunique(),
        "Phase 2 Count (Self Pay)": collected_cases_2["installment_uniqueid"].nunique(),
        "Phase 2 Rate with Working Date (Self Pay)": check2[check2["start_working_date"] < check2["trx_actual_collection_date"]].installment_uniqueid.nunique() / our_cases_2.shape[0],
        "Phase 2 Count with Working Date (Self Pay)": check2[check2["start_working_date"] < check2["trx_actual_collection_date"]].installment_uniqueid.nunique(),
        "Phase 2 Collection Rate (Not Pay)": collected_cases_3["installment_uniqueid"].nunique() / phase2_not_pay["installment_uniqueid"].nunique(),
        "Phase 2 Count (Not Pay)": collected_cases_3["installment_uniqueid"].nunique(),
        "Phase 2 Rate with Working Date (Not Pay)": check3[check3["start_working_date"] < check3["trx_actual_collection_date"]].installment_uniqueid.nunique() / our_cases_3.shape[0],
        "Phase 2 Count with Working Date (Not Pay)": check3[check3["start_working_date"] < check3["trx_actual_collection_date"]].installment_uniqueid.nunique()
    }

# Streamlit app
st.title("Collection Monitoring")

st.sidebar.title("Upload Files")
category = st.sidebar.selectbox("Select Category", ["Card", "Normal"])

result_sql_file = st.sidebar.file_uploader("Upload result_sql.csv", type="csv")
dues_file = st.sidebar.file_uploader(f"Upload {'card_dues.csv' if category == 'Card' else 'normal_dues.csv'}", type="csv")
phase1_file = st.sidebar.file_uploader(f"Upload Phase 1 {'Card' if category == 'Card' else 'Normal'} Excel file", type="xlsx")
phase2_self_pay_file = st.sidebar.file_uploader(f"Upload Phase 2 Self Pay {'Card' if category == 'Card' else 'Normal'} Excel file", type="xlsx")
phase2_not_pay_file = st.sidebar.file_uploader(f"Upload Phase 2 Not Pay {'Card' if category == 'Card' else 'Normal'} Excel file", type="xlsx")

if result_sql_file and dues_file and phase1_file and phase2_self_pay_file and phase2_not_pay_file:
    data = process_data(category, result_sql_file, dues_file, phase1_file, phase2_self_pay_file, phase2_not_pay_file)

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
