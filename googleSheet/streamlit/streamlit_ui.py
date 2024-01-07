# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()

# Write directly to the app
st.title("Google sheet data pipeline")


with st.expander("Write to google sheet"):
    st.write("""
        Write data to google sheet from snowflake table
        """)
    with st.form("write"):
        table = st.text_input(
            "Table name (enter the full table name):",
            placeholder="Table name",
        )
        spreadsheet_id = st.text_input(
            "Google sheet ID: ",
            placeholder="Google sheet ID",
        )
        
        sheet_range = st.text_input(
            "sheet range(sheet1!A:Z): ",
            placeholder="sheet range",
        )
        submitted = st.form_submit_button("Submit")
        if submitted:
            result = session.sql(f'''
                CALL SAMPLE_DATA.TPCH_SF1.WRITE_TO_GSHEET('{table}', '{spreadsheet_id}', '{sheet_range}')
                ''')
            print(result.collect())
            

with st.expander("Read from google sheet to snowflake table"):
    st.write("""Export data from google sheet to snowflake""")
    with st.form("read_sheet"):

        spreadsheet_id = st.text_input(
            "Google sheet ID: ",
            placeholder="Google sheet ID",
        )
        
        sheet_range = st.text_input(
            "sheet range: ",
            placeholder="sheet range",
        )
        table = st.text_input(
            "Enter destination table name",
            placeholder="Table name",
        )
        database = st.text_input(
            "Enter destination database name",
            placeholder="database",
        )
        schema = st.text_input(
            "Enter destination schema name",
            placeholder="schema",
        )
        
        submitted = st.form_submit_button("Submit")
        if submitted:
            result = session.sql(f'''
                CALL SAMPLE_DATA.TPCH_SF1.READ_SHEET('{spreadsheet_id}', '{sheet_range}', '{table}', 
            '{database}', '{schema}')
                ''')
            print(result.collect())
            
