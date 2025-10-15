import pandas as pd
import sqlite3

# --- CONFIGURATION ---
# TODO: Update these three variables to match your setup.

# 1. The path to your SQLite database file.
DATABASE_NAME = 'hungarian_towns.db'

# 2. The path to your Excel file containing the full list of town names.
EXCEL_FILE_PATH = r'C:\Users\uif56391\Desktop\települések.xlsx'  # <-- CHANGE THIS TO YOUR EXCEL FILENAME

# 3. The exact name of the column in your Excel file that contains the town names.
#    This is case-sensitive!
TOWN_NAME_COLUMN = 'Name'  # <-- CHANGE THIS (e.g., 'Település', 'Town Name', etc.)

# --- END OF CONFIGURATION ---


def read_towns_from_excel(file_path: str, column_name: str) -> list[str]:
    """
    Reads a list of town names from a specified column in an Excel file.
    
    Returns:
        A list of cleaned town names, or an empty list if an error occurs.
    """
    try:
        print(f"Reading town names from '{file_path}' (Column: '{column_name}')...")
        df = pd.read_excel(file_path)
        
        if column_name not in df.columns:
            print(f"Error: Column '{column_name}' not found in the Excel file.")
            print(f"Available columns are: {list(df.columns)}")
            return []
            
        # Get the column, drop any empty cells, convert to string, and strip whitespace
        town_names = [str(name).strip() for name in df[column_name].dropna()]
        print(f"Successfully read {len(town_names)} town names from the Excel file.")
        return town_names
        
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return []
    except Exception as e:
        print(f"An unexpected error occurred while reading the Excel file: {e}")
        return []

def upsert_town_names(db_path: str, town_names: list[str]):
    """
    Connects to the SQLite database and inserts new town names.
    If a town name already exists, it is skipped.
    """
    if not town_names:
        print("No town names to process. Exiting.")
        return

    inserted_count = 0
    skipped_count = 0

    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        print(f"\nConnecting to database '{db_path}' to upsert names...")

        # The 'INSERT OR IGNORE' command is a highly efficient way to do this.
        # It attempts to insert a row. If the INSERT fails because the 'name'
        # (which is the PRIMARY KEY) already exists, it simply ignores the
        # command and moves on, without raising an error.
        
        for name in town_names:
            if not name:  # Skip empty names
                continue
                
            cursor.execute("INSERT OR IGNORE INTO towns (name) VALUES (?)", (name,))
            
            # cursor.rowcount will be 1 if a row was inserted, 0 if it was ignored.
            if cursor.rowcount > 0:
                inserted_count += 1
            else:
                skipped_count += 1
        
        # Commit the changes to the database
        conn.commit()
        print("Database changes have been committed.")

    except sqlite3.Error as e:
        print(f"A database error occurred: {e}")
        print("No changes were saved.")
    
    finally:
        if 'conn' in locals() and conn:
            conn.close()

    # --- Final Report ---
    print("\n--- Upsert Process Summary ---")
    print(f"Total names processed from Excel: {len(town_names)}")
    print(f"New towns inserted into database: {inserted_count}")
    print(f"Existing towns skipped: {skipped_count}")
    print("-----------------------------\n")

def main():
    """Main function to run the script."""
    town_list = read_towns_from_excel(EXCEL_FILE_PATH, TOWN_NAME_COLUMN)
    upsert_town_names(DATABASE_NAME, town_list)

if __name__ == '__main__':
    main()