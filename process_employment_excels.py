import pandas as pd
import os
import re
import sqlite3

# --- CONFIGURATION ---
INPUT_DATA_DIR = "employment_excels"
DATABASE_NAME = 'hungarian_towns.db'
TABLE_NAME = 'unemployment_stats'

# --- DATABASE SETUP ---
def setup_database():
    """Sets up the SQLite database and the unemployment stats table."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            town_name TEXT,
            date TEXT,
            unemployed_total INTEGER,
            working_age_population INTEGER,
            unemployment_rate REAL,
            relative_ratio REAL,
            PRIMARY KEY (town_name, date)
        )
    ''')
    conn.commit()
    conn.close()
    print(f"Database '{DATABASE_NAME}' and table '{TABLE_NAME}' are ready.")

# --- PARSE EXCEL FILES & SAVE TO DB ---

def get_date_from_filename(filename):
    """
    Extracts YYYY-MM-01 date string from filename formats like T...202411.xlsx
    """
    # CORRECTED REGEX: Specifically looks for a 20XX year followed by a two-digit month.
    match = re.search(r'(20\d{2})(\d{2})', filename)
    if match:
        year, month = match.groups()
        # Additional check for valid month
        if 1 <= int(month) <= 12:
            return f"{year}-{month}-01"
    print(f"  -> WARNING: Could not parse a valid YYYYMM date from filename: {filename}")
    return None

def process_excel_file(filepath):
    """
    Reads a single badly-formatted Excel file, parses all sheets, cleans the data,
    and returns a single pandas DataFrame for that month.
    """
    filename = os.path.basename(filepath)
    date_str = get_date_from_filename(filename)
    if not date_str:
        return None

    print(f"Processing {filename} for date {date_str}...")

    try:
        all_sheets = pd.read_excel(filepath, sheet_name=None, skiprows=7, header=None, engine='openpyxl')
    except Exception as e:
        print(f"  -> Could not read Excel file: {e}")
        return None

    column_headers = [
        'town_name', 'empty_b', 'empty_c', 'unemployed_total', 
        'unemployed_over_365_days', 'allowance_based_support', 'aid_based_support',
        'fht_rszs_support', 'working_age_population', 'unemployment_rate', 'relative_ratio'
    ]
    
    all_clean_data = []

    for sheet_name, df in all_sheets.items():
        if df.empty or df.shape[1] < len(column_headers):
            continue

        df.columns = column_headers
        df = df.drop(columns=['empty_b', 'empty_c'])
        df = df.dropna(subset=['town_name'])
        df = df[~df['town_name'].astype(str).str.contains("vármegye|főváros|\\*", case=False, na=False)]
        
        df['town_name'] = df['town_name'].astype(str).str.strip().str.upper()
        
        numeric_cols = ['unemployed_total', 'working_age_population', 'unemployment_rate', 'relative_ratio']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

        df = df.dropna(subset=numeric_cols)
        all_clean_data.append(df)

    if not all_clean_data:
        print(f"  -> No valid data found in {filename}")
        return None

    monthly_df = pd.concat(all_clean_data, ignore_index=True)
    monthly_df['date'] = date_str
    
    final_columns = ['town_name', 'date', 'unemployed_total', 'working_age_population', 'unemployment_rate', 'relative_ratio']
    monthly_df = monthly_df[final_columns]
    
    print(f"  -> Successfully processed {len(monthly_df)} rows.")
    return monthly_df

def save_to_db(df):
    """
    Saves the monthly DataFrame to SQLite using a universally compatible
    DELETE-then-INSERT approach to prevent duplicates.
    """
    if df is None or df.empty: return

    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    date_str = df['date'].iloc[0]

    # 1. DELETE any existing records for this specific month.
    # This makes the script safely re-runnable without creating duplicates.
    cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE date = ?", (date_str,))
    
    # 2. INSERT the new, clean data for the month.
    df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
    
    print(f"Saved {len(df)} records for {date_str} to the database after clearing previous entries.")
    conn.commit()
    conn.close()

# --- MAIN EXECUTION ---
def main():
    setup_database()
    
    if not os.path.isdir(INPUT_DATA_DIR):
        print(f"Error: The directory '{INPUT_DATA_DIR}' was not found.")
        print("Please create it and place your Excel files inside.")
        return

    excel_files = [f for f in os.listdir(INPUT_DATA_DIR) if f.endswith('.xlsx')]

    if not excel_files:
        print(f"No Excel (.xlsx) files found in the '{INPUT_DATA_DIR}' directory.")
        return
        
    print(f"\nFound {len(excel_files)} Excel files to process in '{INPUT_DATA_DIR}'.\n")
    
    for filename in sorted(excel_files):
        filepath = os.path.join(INPUT_DATA_DIR, filename)
        monthly_dataframe = process_excel_file(filepath)
        if monthly_dataframe is not None and not monthly_dataframe.empty:
            save_to_db(monthly_dataframe)

    print("\nUnemployment data processing from local files is complete!")

if __name__ == '__main__':
    main()