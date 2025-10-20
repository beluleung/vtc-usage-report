import streamlit as st
from datetime import datetime, timedelta, UTC
import pandas as pd

# Import the functions from your refactored script
import report_generator

# --- Page Configuration ---
# This should be the first Streamlit command in your script
st.set_page_config(
    page_title="VTC OAK Usage Report",
    page_icon="📄",
    layout="centered"
)

# --- NEW FUNCTION TO HIDE STREAMLIT UI ELEMENTS ---
def hide_streamlit_style():
    """Hides the Streamlit footer, menu, and header."""
    hide_style = """
        <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        div[data-testid="stDecoration"] {
            visibility: hidden;
            height: 0%;
            position: fixed;
        }
        </style>
    """
    st.markdown(hide_style, unsafe_allow_html=True)

# --- Main App UI ---

# Display the VTC logo
try:
    st.image("vtc_logo.png", width=200)
except Exception:
    st.warning("vtc_logo.png not found. Please place it in the same directory.")

st.title("VTC OAK Usage Report Generator")

st.markdown("""
Use this tool to generate a usage report for the OAK platform.
Select a date range and the desired format, then click "Generate Report".
""")

# --- Sidebar for User Inputs ---
with st.sidebar:
    st.header("Report Options")

    # Date Range Selection
    # Default to the last 30 days
    today = datetime.now(UTC).date()
    thirty_days_ago = today - timedelta(days=29)

    start_date = st.date_input("Start Date", value=thirty_days_ago)
    end_date = st.date_input("End Date", value=today)

    # Ensure start_date is not after end_date
    if start_date > end_date:
        st.error("Error: Start date must be before or on the end date.")
        st.stop() # Stop the app from running further

    # Format Selection
    report_format = st.radio("Select Report Format", ("Excel", "DOCX"))

    # The "Generate" button
    generate_button = st.button("Generate Report", type="primary")


# --- Report Generation Logic ---
if generate_button:
    # Convert dates to pandas Timestamps, which our functions expect
    start_dt = pd.to_datetime(start_date, utc=True)
    # The end of the day is 23:59:59
    end_dt = pd.to_datetime(end_date, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    # Show a spinner while the data is being fetched and processed
    with st.spinner("Fetching data from DynamoDB and building report..."):
        try:
            # 1. Fetch data using your existing functions
            access_key, secret_key, region = report_generator.load_env_credentials()
            accounts_df = report_generator.get_data_from_dynamodb(report_generator.TABLE_ACCOUNTS, access_key, secret_key, region)
            usage_df_all = report_generator.get_data_from_dynamodb(report_generator.TABLE_USAGE, access_key, secret_key, region)
            askai_df_all = report_generator.get_data_from_dynamodb(report_generator.TABLE_ASKAI, access_key, secret_key, region)

            # 2. Filter data by the selected date range
            usage_df = report_generator.filter_by_date(usage_df_all, start_dt, end_dt)
            askai_df = report_generator.filter_by_date(askai_df_all, start_dt, end_dt)

            # 3. Build the final report DataFrame
            report_df = report_generator.build_report_dataframe(accounts_df, usage_df, askai_df)

            st.success("Report generated successfully!")

            # --- Display a preview of the report ---
            st.subheader("Report Preview")
            st.dataframe(report_df)

            # --- Prepare file for download ---
            file_name_date_part = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

            if report_format == "Excel":
                file_data = report_generator.export_excel(report_df)
                file_name = f"vtc_report_{file_name_date_part}.xlsx"
                mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            else: # DOCX
                file_data = report_generator.export_docx(report_df, start_dt, end_dt)
                file_name = f"vtc_report_{file_name_date_part}.docx"
                mime_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

            # --- Download Button ---
            st.download_button(
                label=f"📥 Download {report_format} Report",
                data=file_data,
                file_name=file_name,
                mime=mime_type,
            )

        except Exception as e:
            st.error(f"An error occurred: {e}")
            st.exception(e) # This will print the full traceback for debugging
