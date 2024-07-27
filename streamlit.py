import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# File uploaders
uploaded_result_sql = st.file_uploader("Upload result_sql.csv", type=["csv"])
uploaded_card = st.file_uploader("Upload card_dues.csv", type=["csv"])

if uploaded_result_sql is not None and uploaded_card is not None:
    # Read CSV files
    result_sql = pd.read_csv(uploaded_result_sql)
    card = pd.read_csv(uploaded_card)
    
    # Define file uploaders for Excel files
    uploaded_phase1_card = st.file_uploader("Upload Phase 1 Card Excel File", type=["xlsx"])
    uploaded_phase2_card = st.file_uploader("Upload Phase 2 Card Excel File", type=["xlsx"])

    if uploaded_phase1_card is not None and uploaded_phase2_card is not None:
        # Read Excel files
        phase1_card = pd.read_excel(uploaded_phase1_card)
        phase2_card = pd.read_excel(uploaded_phase2_card)

        # Streamlit app
        st.title("Customer Payment Analysis Dashboard")

        # Dropdown menu for selection
        category = st.selectbox("Select Category", ["Card", "Normal"])
        phase = st.selectbox("Select Phase", ["Phase 1", "Phase 2"])

        # Load and process data based on selection
        if category == "Card":
            if phase == "Phase 1":
                # Phase 1 Card data
                test_phase1_card = card
                test_phase1_card["installment_uniqueid"] = test_phase1_card["installment_uniqueid"].astype(str)
                
                hopefull_card = phase1_card
                hopefull_card["installment_uniqueid"] = hopefull_card["installment_uniqueid"].astype(str)
                
                our_cases_card = test_phase1_card.merge(hopefull_card[["installment_uniqueid"]], on="installment_uniqueid", how="inner")
                our_cases_card['trx_actual_collection_date'] = pd.to_datetime(our_cases_card['trx_actual_collection_date'])
                
                collected_count_phase1 = our_cases_card[our_cases_card["status"] == "Collected"].installment_uniqueid.nunique()
                total_count_phase1 = hopefull_card.installment_uniqueid.nunique()
                
                st.write(f"Phase 1 - Card:")
                st.write(f"Collected Count: {collected_count_phase1}")
                st.write(f"Total Count: {total_count_phase1}")
                st.write(f"Percentage Collected: {collected_count_phase1 / total_count_phase1:.2%}")
                
            elif phase == "Phase 2":
                # Phase 2 Card data
                test_phase2_card = card
                test_phase2_card["installment_uniqueid"] = test_phase2_card["installment_uniqueid"].astype(str)
                
                phase2_self_pay_card = phase2_card
                phase2_self_pay_card["installment_uniqueid"] = phase2_self_pay_card["installment_uniqueid"].astype(str)
                
                our_cases_2_card = test_phase2_card.merge(phase2_self_pay_card[["installment_uniqueid"]], on="installment_uniqueid", how="inner")
                our_cases_2_card['trx_actual_collection_date'] = pd.to_datetime(our_cases_2_card['trx_actual_collection_date'])
                
                collected_count_phase2 = our_cases_2_card[our_cases_2_card["status"] == "Collected"].installment_uniqueid.nunique()
                total_count_phase2 = our_cases_2_card.installment_uniqueid.nunique()
                
                st.write(f"Phase 2 - Card:")
                st.write(f"Collected Count: {collected_count_phase2}")
                st.write(f"Total Count: {total_count_phase2}")
                st.write(f"Percentage Collected: {collected_count_phase2 / total_count_phase2:.2%}")

        elif category == "Normal":
            st.write("Normal category data not available in this example.")

        # Visualization
        if category == "Card":
            if phase == "Phase 1":
                fig, ax = plt.subplots()
                ax.bar(["Collected", "Total"], [collected_count_phase1, total_count_phase1])
                ax.set_title('Phase 1 Card Collection')
                st.pyplot(fig)
            elif phase == "Phase 2":
                fig, ax = plt.subplots()
                ax.bar(["Collected", "Total"], [collected_count_phase2, total_count_phase2])
                ax.set_title('Phase 2 Card Collection')
                st.pyplot(fig)
    else:
        st.info("Please upload both Phase 1 and Phase 2 Card Excel files.")
else:
    st.info("Please upload both CSV files.")
