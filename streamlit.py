import streamlit as st
import pandas as pd
import numpy as np

def load_data():
    st.sidebar.header("Upload your data files")
    
    result_sql_file = st.sidebar.file_uploader("Upload result_sql.csv", type=["csv"])
    card_file = st.sidebar.file_uploader("Upload card_dues.csv", type=["csv"])
    
    if result_sql_file is not None:
        st.write("Result SQL file uploaded")
        result_sql = pd.read_csv(result_sql_file)
    else:
        st.write("Result SQL file not uploaded")
        return None, None
    
    if card_file is not None:
        st.write("Card file uploaded")
        card = pd.read_csv(card_file)
    else:
        st.write("Card file not uploaded")
        return None, None
    
    return result_sql, card


# Function to process Card data for Phase 1
def process_card_phase1(card):
    hopefull_card = pd.read_excel("Card_Solution1_Will_Pay_Alone.xlsx")
    hopefull_card["installment_uniqueid"] = hopefull_card["installment_uniqueid"].astype(str)
    our_cases_card = card.merge(hopefull_card[["installment_uniqueid"]], on="installment_uniqueid", how="inner")
    our_cases_card['trx_actual_collection_date'] = pd.to_datetime(our_cases_card['trx_actual_collection_date'])
    our_cases_card['trx_actual_collection_date_only'] = our_cases_card['trx_actual_collection_date'].dt.date
    return our_cases_card

# Function to process Card data for Phase 2
def process_card_phase2(card):
    phase2_self_pay_card = pd.read_excel("Card_Solution2_Will_Pay_Alone.xlsx")
    phase2_self_pay_card["installment_uniqueid"] = phase2_self_pay_card["installment_uniqueid"].astype(str)
    our_cases_2_card = card.merge(phase2_self_pay_card[["installment_uniqueid"]], on="installment_uniqueid", how="inner")
    our_cases_2_card['trx_actual_collection_date'] = pd.to_datetime(our_cases_2_card['trx_actual_collection_date'])
    our_cases_2_card['trx_actual_collection_date_only'] = our_cases_2_card['trx_actual_collection_date'].dt.date
    return our_cases_2_card

# Function to process Phase 2 with "Will Not Pay"
def process_card_phase2_not_pay(card):
    phase2_not_pay_card = pd.read_excel("Card_Solution2_Will_Not_Pay.xlsx")
    phase2_not_pay_card["installment_uniqueid"] = phase2_not_pay_card["installment_uniqueid"].astype(str)
    our_cases_3_card = card.merge(phase2_not_pay_card[["installment_uniqueid"]], on="installment_uniqueid", how="inner")
    our_cases_3_card['trx_actual_collection_date'] = pd.to_datetime(our_cases_3_card['trx_actual_collection_date'])
    our_cases_3_card['trx_actual_collection_date_only'] = our_cases_3_card['trx_actual_collection_date'].dt.date
    return our_cases_3_card

# Function to compare phases
def compare_phases(df_phase1, df_phase2):
    collected_phase1 = df_phase1[df_phase1['status'] == 'Collected'].installment_uniqueid.nunique()
    collected_phase2 = df_phase2[df_phase2['status'] == 'Collected'].installment_uniqueid.nunique()
    return collected_phase1, collected_phase2

# Streamlit application
def main():
    st.title('Data Analysis Dashboard')

    # Load data
    result_sql, card = load_data()

    # Dropdown menu for user input
    category = st.selectbox("Select Category", ["Card", "Normal"])
    phase = st.selectbox("Select Phase", ["Phase 1", "Phase 2"])

    # Process data based on user input
    if category == "Card":
        if phase == "Phase 1":
            df = process_card_phase1(card)
        elif phase == "Phase 2":
            df = process_card_phase2(card)
    else:
        st.write("Normal category processing not implemented.")

    # Data comparison between phases
    if st.button('Compare Phases'):
        if phase == "Phase 1":
            df_phase1 = process_card_phase1(card)
            df_phase2 = process_card_phase2(card)
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
