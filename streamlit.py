import streamlit as st
import pandas as pd
from datetime import datetime

# Function to process Card data
def process_card_data(files):
    try:
        result_sql = pd.read_csv(files['result_sql.csv'])
    except Exception as e:
        st.error(f"Error reading result_sql.csv: {e}")
        return None

    try:
        card = pd.read_csv(files['card_dues.csv'])
    except Exception as e:
        st.error(f"Error reading card_dues.csv: {e}")
        return None

    try:
        hopefull_card = pd.read_excel(files['Phase 1 Card.xlsx'])
    except Exception as e:
        st.error(f"Error reading Phase 1 Card.xlsx: {e}")
        return None

    try:
        phase2_self_pay_card = pd.read_excel(files['Phase 2 Self Pay Card.xlsx'])
    except Exception as e:
        st.error(f"Error reading Phase 2 Self Pay Card.xlsx: {e}")
        return None

    try:
        phase2_not_pay_card = pd.read_excel(files['Phase 2 Not Pay Card.xlsx'])
    except Exception as e:
        st.error(f"Error reading Phase 2 Not Pay Card.xlsx: {e}")
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
        "Phase 2 Collection Rate (Not Pay)": collected_cases_3_card["installment_uniqueid"].nunique() / phase2_not_pay_card["installment_uniqueid"].nunique(),
        "Phase 2 Count (Not Pay)": collected_cases_3_card["installment_uniqueid"].nunique(),
        "Phase 2 Rate with Working Date (Not Pay)": check3_card[check3_card["start_working_date"] < check3_card["trx_actual_collection_date"]].installment_uniqueid.nunique() / our_cases_3_card.shape[0],
        "Phase 2 Count with Working Date (Not Pay)": check3_card[check3_card["start_working_date"] < check3_card["trx_actual_collection_date"]].installment_uniqueid.nunique()
    }

# Function to process Normal data
def process_normal_data(files):
    try:
        result_sql = pd.read_csv(files['result_sql.csv'])
    except Exception as e:
        st.error(f"Error reading result_sql.csv: {e}")
        return None

    try:
        normal = pd.read_csv(files['normal_dues.csv'])
    except Exception as e:
        st.error(f"Error reading normal_dues.csv: {e}")
        return None

    try:
        hopefull_normal = pd.read_excel(files['Phase 1 Normal.xlsx'])
    except Exception as e:
        st.error(f"Error reading Phase 1 Normal.xlsx: {e}")
        return None

    try:
        phase2_self_pay_normal = pd.read_excel(files['Phase 2 Self Pay Normal.xlsx'])
    except Exception as e:
        st.error(f"Error reading Phase 2 Self Pay Normal.xlsx: {e}")
        return None

    try:
        phase2_not_pay_normal = pd.read_excel(files['Phase 2 Not Pay Normal.xlsx'])
    except Exception as e:
        st.error(f"Error reading Phase 2 Not Pay Normal.xlsx: {e}")
        return None

    normal["installment_uniqueid"] = normal["installment_uniqueid"].astype(str)
    hopefull_normal["installment_uniqueid"] = hopefull_normal["installment_uniqueid"].astype(str)
    phase2_self_pay_normal["installment_uniqueid"] = phase2_self_pay_normal["installment_uniqueid"].astype(str)
    phase2_not_pay_normal["installment_uniqueid"] = phase2_not_pay_normal["installment_uniqueid"].astype(str)

    our_cases_normal = normal.merge(
        hopefull_normal[["installment_uniqueid"]], on="installment_uniqueid", how="inner"
    )
    our_cases_normal['trx_actual_collection_date'] = pd.to_datetime(our_cases_normal['trx_actual_collection_date'])
    our_cases_normal['trx_actual_collection_date_only'] = our_cases_normal['trx_actual_collection_date'].dt.date

    collected_cases_normal = our_cases_normal[our_cases_normal["status"] == "Collected"]
    ids_with_dates_normal = collected_cases_normal[["installment_uniqueid", "trx_actual_collection_date"]].drop_duplicates()

    result_sql["start_working_date"] = pd.to_datetime(result_sql["start_working_date"])
    result_sql["installment_uniqueid"] = result_sql["installment_uniqueid"].astype(str)

    start_date = pd.Timestamp("2024-07-05")
    filtered_result_sql = result_sql[result_sql["start_working_date"] >= start_date]
    check_normal = filtered_result_sql.merge(ids_with_dates_normal, on="installment_uniqueid", how="inner")

    test_phase2_normal = normal.copy()
    our_cases_2_normal = test_phase2_normal.merge(
        phase2_self_pay_normal[["installment_uniqueid"]], on="installment_uniqueid", how="inner"
    )
    our_cases_2_normal['trx_actual_collection_date'] = pd.to_datetime(our_cases_2_normal['trx_actual_collection_date'])
    our_cases_2_normal['trx_actual_collection_date_only'] = our_cases_2_normal['trx_actual_collection_date'].dt.date

    collected_cases_2_normal = our_cases_2_normal[our_cases_2_normal["status"] == "Collected"]
    ids_with_dates2_normal = collected_cases_2_normal[["installment_uniqueid", "trx_actual_collection_date"]].drop_duplicates()
    check2_normal = filtered_result_sql.merge(ids_with_dates2_normal, on="installment_uniqueid", how="inner")

    our_cases_3_normal = test_phase2_normal.merge(
        phase2_not_pay_normal[["installment_uniqueid"]], on="installment_uniqueid", how="inner"
    )
    our_cases_3_normal['trx_actual_collection_date'] = pd.to_datetime(our_cases_3_normal['trx_actual_collection_date'])
    our_cases_3_normal['trx_actual_collection_date_only'] = our_cases_3_normal['trx_actual_collection_date'].dt.date

    collected_cases_3_normal = our_cases_3_normal[our_cases_3_normal["status"] == "Collected"]
    ids_with_dates3_normal = collected_cases_3_normal[["installment_uniqueid", "trx_actual_collection_date"]].drop_duplicates()
    check3_normal = filtered_result_sql.merge(ids_with_dates3_normal, on="installment_uniqueid", how="inner")

    return {
        "Phase 1 Collection Rate": collected_cases_normal["installment_uniqueid"].nunique() / hopefull_normal["installment_uniqueid"].nunique(),
        "Phase 1 Count": collected_cases_normal["installment_uniqueid"].nunique(),
        "Phase 1 Rate with Working Date": check_normal[check_normal["start_working_date"] < check_normal["trx_actual_collection_date"]].installment_uniqueid.nunique() / hopefull_normal.shape[0],
        "Phase 1 Count with Working Date": check_normal[check_normal["start_working_date"] < check_normal["trx_actual_collection_date"]].installment_uniqueid.nunique(),
        "Phase 2 Collection Rate (Self Pay)": collected_cases_2_normal["installment_uniqueid"].nunique() / phase2_self_pay_normal["installment_uniqueid"].nunique(),
        "Phase 2 Count (Self Pay)": collected_cases_2_normal["installment_uniqueid"].nunique(),
        "Phase 2 Rate with Working Date (Self Pay)": check2_normal[check2_normal["start_working_date"] < check2_normal["trx_actual_collection_date"]].installment_uniqueid.nunique() / our_cases_2_normal.shape[0],
        "Phase 2 Count with Working Date (Self Pay)": check2_normal[check2_normal["start_working_date"] < check2_normal["trx_actual_collection_date"]].installment_uniqueid.nunique(),
        "Phase 2 Collection Rate (Not Pay)": collected_cases_3_normal["installment_uniqueid"].nunique() / phase2_not_pay_normal["installment_uniqueid"].nunique(),
        "Phase 2 Count (Not Pay)": collected_cases_3_normal["installment_uniqueid"].nunique(),
        "Phase 2 Rate with Working Date (Not Pay)": check3_normal[check3_normal["start_working_date"] < check3_normal["trx_actual_collection_date"]].installment_uniqueid.nunique() / our_cases_3_normal.shape[0],
        "Phase 2 Count with Working Date (Not Pay)": check3_normal[check3_normal["start_working_date"] < check3_normal["trx_actual_collection_date"]].installment_uniqueid.nunique()
    }

st.sidebar.title("Upload Excel Files")
uploaded_files = {
    'result_sql.csv': st.sidebar.file_uploader("Upload result_sql.csv", type=["csv"]),
    'card_dues.csv': st.sidebar.file_uploader("Upload card_dues.csv", type=["csv"]),
    'normal_dues.csv': st.sidebar.file_uploader("Upload normal_dues.csv", type=["csv"]),
    'Phase 1 Card.xlsx': st.sidebar.file_uploader("Upload Phase 1 Card.xlsx", type=["xlsx"]),
    'Phase 2 Self Pay Card.xlsx': st.sidebar.file_uploader("Upload Phase 2 Self Pay Card.xlsx", type=["xlsx"]),
    'Phase 2 Not Pay Card.xlsx': st.sidebar.file_uploader("Upload Phase 2 Not Pay Card.xlsx", type=["xlsx"]),
    'Phase 1 Normal.xlsx': st.sidebar.file_uploader("Upload Phase 1 Normal.xlsx", type=["xlsx"]),
    'Phase 2 Self Pay Normal.xlsx': st.sidebar.file_uploader("Upload Phase 2 Self Pay Normal.xlsx", type=["xlsx"]),
    'Phase 2 Not Pay Normal.xlsx': st.sidebar.file_uploader("Upload Phase 2 Not Pay Normal.xlsx", type=["xlsx"])
}

category = st.selectbox("Select Category", ["Card", "Normal"])

if category == "Card":
    if all(uploaded_files[key] for key in ['result_sql.csv', 'card_dues.csv', 'Phase 1 Card.xlsx', 'Phase 2 Self Pay Card.xlsx', 'Phase 2 Not Pay Card.xlsx']):
        card_data = process_card_data(uploaded_files)
        if card_data:
            st.write("Card Data Metrics")
            st.write(card_data)
    else:
        st.warning("Please upload all Card related files.")
elif category == "Normal":
    if all(uploaded_files[key] for key in ['result_sql.csv', 'normal_dues.csv', 'Phase 1 Normal.xlsx', 'Phase 2 Self Pay Normal.xlsx', 'Phase 2 Not Pay Normal.xlsx']):
        normal_data = process_normal_data(uploaded_files)
        if normal_data:
            st.write("Normal Data Metrics")
            st.write(normal_data)
    else:
        st.warning("Please upload all Normal related files.")
