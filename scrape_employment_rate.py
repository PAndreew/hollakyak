import hrequests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import sqlite3

BASE_URL = "https://nfsz.munka.hu"
# *** THIS IS THE CORRECTED URL ***
# We are now targeting the specific API endpoint that the JavaScript calls.
API_ENDPOINT_URL = f"{BASE_URL}/common/service/requestparser"
VISIBLE_PAGE_URL = f"{BASE_URL}/tart/stat_telepulessoros_adatok"
DOWNLOAD_DIR = "unemployment_data"
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
            PRIMARY KEY (town_name, date)
        )
    ''')
    conn.commit()
    conn.close()
    print(f"Database '{DATABASE_NAME}' and table '{TABLE_NAME}' are ready.")

# --- STEP 1: FIND & DOWNLOAD EXCEL FILES ---

def get_excel_links():
    """Scrapes the NFSZ page to find all relevant Excel file links from 2023 onwards."""
    print("Initiating session with manual cookie handling...")
    
    payload = {
        'service': 'ajax',
        'request': 'getHTML',
        'params[plugin]': 'hirfolyam',
        'params[felulet]': 'stat_telepulessoros',
        'params[db]': '50',
        'params[page]': '1'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Referer': VISIBLE_PAGE_URL,
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': BASE_URL
    }

    try:
        # Step 1 & 2: Visit the page and capture the cookies
        print(f"  -> GET {VISIBLE_PAGE_URL} to obtain session cookies...")
        get_response = hrequests.get(VISIBLE_PAGE_URL, headers=headers)
        if get_response.status_code != 200:
            print(f"Failed to fetch {API_ENDPOINT_URL}. Status code: {get_response.status_code}")
            return None
        session_cookies = get_response.cookies
        print(f"  -> Cookies received: {session_cookies}")

        # Step 3: Make the API call, explicitly passing the captured cookies
        print(f"  -> POST {API_ENDPOINT_URL} with captured cookies...")
        post_response = hrequests.post(
            API_ENDPOINT_URL,
            data=payload,
            headers=headers,
            cookies=session_cookies  # <-- This is the crucial part
        )
        if post_response.status_code != 200:
            print(f"Failed to fetch {API_ENDPOINT_URL}. Status code: {post_response.status_code}")
            return None
        print(post_response.headers)
        data = post_response.json()
        print(data)
    except hrequests.exceptions.ClientException as e:
        print(f"Error fetching page: {e}")
        return []

    links = []
    for item in data:
        # The key for the file path is 'DOC_URL_PUB'
        href = item.get('DOC_URL_PUB', '')
        
        # We only want Excel files, not PDFs
        if '.xlsx' not in href:
            continue

        # Use regex to find the year and filter for 2023 onwards
        match = re.search(r'(\d{4})', href)
        if match:
            year = int(match.group(1))
            if year >= 2023:
                full_url = BASE_URL + href
                links.append(full_url)

    if not links:
        print("WARNING: No Excel links were found. The website's API may have changed.")
    else:
        print(f"Found {len(links)} relevant Excel files (2023-present) via API.")
        
    return list(set(links))

def download_files(links):
    """Downloads files from a list of URLs into the DOWNLOAD_DIR if they don't already exist."""
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        print(f"Created directory: {DOWNLOAD_DIR}")

    downloaded_paths = []
    for link in links:
        filename = link.split('/')[-1]
        filepath = os.path.join(DOWNLOAD_DIR, filename)
        
        if not os.path.exists(filepath):
            print(f"Downloading {filename}...")
            try:
                response = hrequests.get(link, stream=True)
                if response.status_code != 200:
                    print(f"Failed to fetch {link}. Status code: {response.status_code}")
                    return None
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                downloaded_paths.append(filepath)
            except hrequests.exceptions.ClientException as e:
                print(f"  -> Failed to download {filename}: {e}")
        else:
            print(f"Skipping download, {filename} already exists.")
            downloaded_paths.append(filepath)
            
    return downloaded_paths

# --- STEP 2: PARSE EXCEL FILES & SAVE TO DB ---

def process_excel_file(filepath):
    """
    Reads a single Excel file, parses all sheets, cleans the data,
    and returns a single pandas DataFrame for that month.
    """
    filename = os.path.basename(filepath)
    match = re.search(r'T01(\d{4})(\d{2})\.xlsx', filename)
    if not match:
        return None
        
    year, month = match.groups()
    date_str = f"{year}-{month}-01"
    print(f"Processing {filename} for date {date_str}...")

    try:
        # Read all sheets from the Excel file into a dictionary of DataFrames
        all_sheets = pd.read_excel(filepath, sheet_name=None, skiprows=7, engine='openpyxl')
    except Exception as e:
        print(f"  -> Could not read Excel file: {e}")
        return None

    all_data = []
    
    # Define the column mapping to standardize names
    column_mapping = {
        'települései': 'town_name',
        'Nyilvántar-tott össz. fő': 'unemployed_total',
        'Munkav. korú népes. fő*': 'working_age_population',
        'Relatív mutató** %': 'unemployment_rate'
    }
    
    for sheet_name, df in all_sheets.items():
        # Check if the required columns exist by checking the first column name
        if df.columns[0] in column_mapping:
            
            # Select and rename columns we care about
            df = df.rename(columns=column_mapping)
            df = df[list(column_mapping.values())]

            # Drop rows with no town name (footers, empty rows)
            df = df.dropna(subset=['town_name'])
            
            # Normalize town names (e.g., "ABALIGET" -> "Abaliget")
            df['town_name'] = df['town_name'].str.strip().str.title()
            
            # Convert unemployment rate from '4,35' string to 4.35 float
            df['unemployment_rate'] = df['unemployment_rate'].astype(str).str.replace(',', '.').astype(float)
            
            # Ensure other columns are numeric, coercing errors to NaN
            for col in ['unemployed_total', 'working_age_population']:
                 df[col] = pd.to_numeric(df[col], errors='coerce')

            all_data.append(df)

    if not all_data:
        print(f"  -> No valid data found in {filename}")
        return None

    # Combine data from all sheets into one DataFrame
    monthly_df = pd.concat(all_data, ignore_index=True)
    monthly_df['date'] = date_str
    
    print(f"  -> Successfully processed {len(monthly_df)} rows.")
    return monthly_df


def save_to_db(df, date_str):
    """Saves the monthly DataFrame to the SQLite database."""
    if df is None or df.empty:
        return

    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # To ensure data integrity, delete any existing records for this month before inserting new ones.
    # This makes the script safely re-runnable with updated Excel files.
    cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE date = ?", (date_str,))
    
    df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
    
    print(f"Saved {len(df)} records for {date_str} to the database.")
    conn.commit()
    conn.close()

# --- MAIN EXECUTION ---

def main():
    """Main function to run the entire scraping and processing pipeline."""
    setup_database()
    
    links = get_excel_links()
    if not links:
        print("No links found. Exiting.")
        return
        
    filepaths = download_files(links)
    
    for path in filepaths:
        monthly_dataframe = process_excel_file(path)
        if monthly_dataframe is not None:
            # Get the date from the DataFrame to use for the delete operation
            date_for_db = monthly_dataframe['date'].iloc[0]
            save_to_db(monthly_dataframe, date_for_db)

    print("\nUnemployment data scraping and processing complete!")

if __name__ == '__main__':
    main()