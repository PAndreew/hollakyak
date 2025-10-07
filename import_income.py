import sqlite3
import pandas as pd

DATABASE_NAME = 'hungarian_towns.db'
EXCEL_FILE_PATH = 'átlagkereset.xlsx' # Make sure this matches your file name

def setup_income_table():
    """Creates the new county_income table in the database if it doesn't exist."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS county_income (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            county_name TEXT NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            average_income INTEGER NOT NULL,
            UNIQUE(county_name, year, quarter)
        )
    ''')
    conn.commit()
    conn.close()
    print("`county_income` table created or already exists.")

def process_and_insert_income_data():
    """Reads the Excel file, transforms the data, and inserts it into the database."""
    try:
        # 1. Read the Excel file into a pandas DataFrame
        df = pd.read_excel(EXCEL_FILE_PATH, engine='calamine')
        print(f"Successfully loaded {EXCEL_FILE_PATH} with {len(df)} rows.")

        # 2. Transform the data from wide to long format
        # id_vars are the columns we want to keep as identifiers
        # value_vars are the columns that will be "unpivoted"
        id_vars = ['Name', 'Type']
        value_vars = [col for col in df.columns if 'Q' in str(col)]

        df_long = pd.melt(df, id_vars=id_vars, value_vars=value_vars,
                          var_name='period', value_name='average_income')

        # 3. Clean and prepare the data for insertion
        # Remove any rows where income is not a number
        df_long.dropna(subset=['average_income'], inplace=True)
        
        # Split 'period' (e.g., '21Q1') into 'year' and 'quarter'
        df_long['year'] = '20' + df_long['period'].astype(str).str[:2]
        df_long['quarter'] = df_long['period'].astype(str).str[3]

        # Convert columns to the correct data types
        df_long['year'] = df_long['year'].astype(int)
        df_long['quarter'] = df_long['quarter'].astype(int)
        
        # Handle potential spaces in numbers (e.g., '396 509')
        if df_long['average_income'].dtype == 'object':
            df_long['average_income'] = df_long['average_income'].astype(str).str.replace(' ', '').astype(int)
        else:
            df_long['average_income'] = df_long['average_income'].astype(int)

        # Rename 'Name' to 'county_name' to match our database schema
        df_long.rename(columns={'Name': 'county_name'}, inplace=True)
        
        # Select only the columns we need for the database
        df_to_insert = df_long[['county_name', 'year', 'quarter', 'average_income']]
        
        print(f"Data transformed. Ready to insert {len(df_to_insert)} records.")

        # 4. Insert data into the SQLite table
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        insert_count = 0
        for index, row in df_to_insert.iterrows():
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO county_income (county_name, year, quarter, average_income)
                    VALUES (?, ?, ?, ?)
                ''', (row['county_name'], row['year'], row['quarter'], row['average_income']))
                insert_count += 1
            except sqlite3.Error as e:
                print(f"Error inserting row {index}: {e}")

        conn.commit()
        conn.close()
        
        print(f"Successfully inserted/updated {insert_count} records into the `county_income` table.")

    except FileNotFoundError:
        print(f"Error: The file '{EXCEL_FILE_PATH}' was not found.")
        print("Please make sure the Excel file is in the same directory and the name is correct.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    setup_income_table()
    process_and_insert_income_data()