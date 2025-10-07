import hrequests
from bs4 import BeautifulSoup
import sqlite3
import re

DATABASE_NAME = 'hungarian_towns.db'
BASE_WIKI_URL = 'https://hu.wikipedia.org'
TOWN_LIST_URL = 'https://hu.wikipedia.org/wiki/Magyarorsz%C3%A1g_telep%C3%BCl%C3%A9sei:_A,_%C3%81'

def setup_database():
    """Sets up the SQLite database and table."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS towns (
            name TEXT PRIMARY KEY,
            type TEXT,
            county TEXT,
            kisterseg TEXT,
            jaras TEXT,
            population INTEGER,
            zip_code TEXT,
            mayor TEXT,
            latitude REAL,
            longitude REAL
        )
    ''')
    conn.commit()
    conn.close()

def get_soup(url):
    """Fetches a URL and returns a BeautifulSoup object."""
    try:
        response = hrequests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch {url}. Status code: {response.status_code}")
            return None
        return BeautifulSoup(response.text, 'html.parser')
    except hrequests.exceptions.ClientException as e:
        print(f"Error fetching {url}: {e}")
        return None

def scrape_town_list_page(soup):
    """
    Scrapes a single town list page (e.g., A, Á) for town details and links.
    Returns a list of dictionaries, each representing a town.
    """
    towns_data = []
    table = soup.find('table', class_='wikitable sortable')

    if not table:
        print("Could not find the main town table on the page.")
        return towns_data

    rows = table.find('tbody').find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 7:
            town_name_tag = cols[0].find('a')
            if town_name_tag:
                town_name = town_name_tag.get_text(strip=True)
                town_link = town_name_tag['href']

                town_type = cols[1].get_text(strip=True)
                county = cols[2].get_text(strip=True)
                kisterseg = cols[3].get_text(strip=True)
                jaras = cols[4].get_text(strip=True)
                
                # Population might have commas, remove them
                population_str = cols[5].get_text(strip=True).replace('.', '')
                population = int(population_str) if population_str.isdigit() else None
                
                # Zip code might have ranges (e.g., 8127–8128), take the first one
                zip_code_raw = cols[6].get_text(strip=True)
                zip_code = zip_code_raw.split('–')[0].split('-')[0].strip()

                towns_data.append({
                    'name': town_name,
                    'link': BASE_WIKI_URL + town_link,
                    'type': town_type,
                    'county': county,
                    'kisterseg': kisterseg,
                    'jaras': jaras,
                    'population': population,
                    'zip_code': zip_code,
                    'mayor': None,  # Will be filled from individual page
                    'latitude': None, # Will be filled from individual page
                    'longitude': None # Will be filled from individual page
                })
    return towns_data

def scrape_individual_town_page(town_url):
    """
    Scrapes an individual town page for mayor's name and GPS coordinates.
    Returns a tuple (mayor, latitude, longitude).
    """
    soup = get_soup(town_url)
    if not soup:
        return None, None, None

    mayor = None
    latitude = None
    longitude = None

    # Find mayor
    infobox = soup.find('table', class_='infobox ujinfobox')
    if infobox:
        mayor_row = infobox.find('td', class_='cimke', string='Polgármester')
        if mayor_row:
            next_td = mayor_row.find_next_sibling('td')
            if next_td:
                mayor = next_td.get_text(strip=True).split('(')[0].strip() # Take only the name before the party

        # Find GPS coordinates
        # Look for the 'geo' span with latitude and longitude
        geo_span = infobox.find('span', class_='geo')
        if geo_span:
            lat_span = geo_span.find('span', class_='latitude')
            lon_span = geo_span.find('span', class_='longitude')
            if lat_span and lon_span:
                try:
                    # Coordinates can be in "é. sz. 47° 01′ 50″" or "47.030650°N" format
                    # Prioritize the decimal format if available
                    dec_coords = geo_span.find('span', class_='geo-dec')
                    if dec_coords:
                        match = re.search(r'(-?\d+\.?\d*)°[NS]\s*(-?\d+\.?\d*)°[EW]', dec_coords.get_text())
                        if match:
                            latitude = float(match.group(1))
                            longitude = float(match.group(2))
                    
                    if latitude is None or longitude is None:
                        # Fallback to DMS if decimal isn't found or parsed
                        lat_dms = lat_span.get_text(strip=True)
                        lon_dms = lon_span.get_text(strip=True)
                        latitude = convert_dms_to_decimal(lat_dms)
                        longitude = convert_dms_to_decimal(lon_dms)

                except ValueError:
                    print(f"Could not parse GPS for {town_url}")

    return mayor, latitude, longitude

def convert_dms_to_decimal(dms_str):
    """Converts a DMS string (e.g., 'é. sz. 47° 01′ 50″') to decimal degrees."""
    
    # Remove direction and other non-numeric characters for easier parsing
    clean_dms_str = re.sub(r'[é\. sz\.k\.h°′″]', '', dms_str)
    parts = clean_dms_str.split()
    
    degrees = float(parts[0]) if len(parts) > 0 else 0
    minutes = float(parts[1]) if len(parts) > 1 else 0
    seconds = float(parts[2]) if len(parts) > 2 else 0
    
    decimal = degrees + minutes / 60 + seconds / 3600
    
    # Check for negative south/west (though Hungarian Wikipedia uses 'é. sz.' and 'k. h.')
    if 'd.' in dms_str: # Southern latitude
        decimal = -decimal
    if 'ny.' in dms_str: # Western longitude
        decimal = -decimal

    return decimal


def insert_town_data(town_data):
    """Inserts a single town's data into the database."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO towns (name, type, county, kisterseg, jaras, population, zip_code, mayor, latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            town_data['name'],
            town_data['type'],
            town_data['county'],
            town_data['kisterseg'],
            town_data['jaras'],
            town_data['population'],
            town_data['zip_code'],
            town_data['mayor'],
            town_data['latitude'],
            town_data['longitude']
        ))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error inserting data for {town_data['name']}: {e}")
    finally:
        conn.close()

def main():
    setup_database()
    
    # Get the initial A, Á page to find all letter links
    main_soup = get_soup(TOWN_LIST_URL)
    if not main_soup:
        return

    letter_links = []
    toc_table = main_soup.find('table', id='toc')
    if toc_table:
        for a_tag in toc_table.find_all('a', href=True):
            if a_tag['href'].startswith('/wiki/Magyarorsz%C3%A1g_telep%C3%BCl%C3%A9sei:'):
                letter_links.append(BASE_WIKI_URL + a_tag['href'])
    
    # Ensure the first page is included if not explicitly found by the loop
    if TOWN_LIST_URL not in letter_links:
        letter_links.insert(0, TOWN_LIST_URL)

    all_towns = []
    for link in sorted(list(set(letter_links))): # Use set to avoid duplicates and sort for consistent processing
        print(f"Scraping town list from: {link}")
        list_soup = get_soup(link)
        if list_soup:
            towns_on_page = scrape_town_list_page(list_soup)
            all_towns.extend(towns_on_page)

    print(f"Found {len(all_towns)} towns in total. Now scraping individual town pages...")

    for i, town in enumerate(all_towns):
        print(f"({i+1}/{len(all_towns)}) Scraping data for {town['name']} from {town['link']}")
        mayor, latitude, longitude = scrape_individual_town_page(town['link'])
        town['mayor'] = mayor
        town['latitude'] = latitude
        town['longitude'] = longitude
        insert_town_data(town)
    
    print("Scraping complete. Data stored in hungarian_towns.db")

if __name__ == '__main__':
    main()