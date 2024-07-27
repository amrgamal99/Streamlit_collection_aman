import streamlit as st
import pandas as pd
from io import StringIO, BytesIO

# Function to load data
def load_data():
    st.sidebar.header("Upload your data files")
    
    result_sql_file = st.sidebar.file_uploader("Upload result_sql.csv", type=["csv"])
    card_file = st.sidebar.file_uploader("Upload card_dues.csv", type=["csv"])
    
    if result_sql_file is not None and card_file is not None:
        result_sql = pd.read_csv(result_sql_file)
        card = pd.read_csv(card_file)
        return result_sql, card
    else:
        st.sidebar.warning("Please upload both files.")
        return None, None

# Function to process Card data for Phase 1
def process_card_phase1(card_file):
    if card_file is not None:
        card = pd.read_csv(card_file)
        st.write("Card file uploaded successfully.")
        
        hopefull_card = st.file_uploader("Upload Card Phase 1 Excel file", type=["xlsx"])
        if hopefull_card is not None:
            hopefull_card_df = pd.read_excel(hopefull_card)
            hopefull_card_df["installment_uniqueid"] = hopefull_card_df["installment_uniqueid"].astype(str)
            our_cases_card = card.merge(hopefull_card_df[["installment_uniqueid"]], on="installment_uniqueid", how="inner")
            our_cases_card['trx_actual_collection_date'] = pd.to_datetime(our_cases_card['trx_actual_collection_date'])
            our_cases_card['trx_actual_collection_date_only'] = our_cases_card['trx_actual_collection_date'].dt.date
            return our_cases_card
        else:
            st.write("Please upload the Card Phase 1 Excel file.")
            return None
    else:
        st.write("Please upload the Card CSV file.")
        return None

# Function to process Card data for Phase 2
def process_card_phase2(card_file):
    if card_file is not None:
        card = pd.read_csv(card_file)
        st.write("Card file uploaded successfully.")
        
        phase2_self_pay_card = st.file_uploader("Upload Card Phase 2 Excel file", type=["xlsx"])
        if phase2_self_pay_card is not None:
            phase2_self_pay_card_df = pd.read_excel(phase2_self_pay_card)
            phase2_self_pay_card_df["installment_uniqueid"] = phase2_self_pay_card_df["installment_uniqueid"].astype(str)
            our_cases_2_card = card.merge(phase2_self_pay_card_df[["installment_uniqueid"]], on="installment_uniqueid", how="inner")
            our_cases_2_card['trx_actual_collection_date'] = pd.to_datetime(our_cases_2_card['trx_actual_collection_date'])
            our_cases_2_card['trx_actual_collection_date_only'] = our_cases_2_card['trx_actual_collection_date'].dt.date
            return our_cases_2_card
        else:
            st.write("Please upload the Card Phase 2 Excel file.")
            return None
    else:
        st.write("Please upload the Card CSV file.")
        return None

def main():
    st.title('Data Analysis Dashboard')

    result_sql, card = load_data()
    
    if result_sql is not None and card is not None:
        category = st.selectbox("Select Category", ["Card", "Normal"])
        phase = st.selectbox("Select Phase", ["Phase 1", "Phase 2"])

        if category == "Card":
            if phase == "Phase 1":
                df_phase1 = process_card_phase1(card)
                if df_phase1 is not None:
                    st.write(df_phase1.head())  # Display a preview of the data

            elif phase == "Phase 2":
                df_phase2 = process_card_phase2(card)
                if df_phase2 is not None:
                    st.write(df_phase2.head())  # Display a preview of the data

if __name__ == "__main__":
    main()
