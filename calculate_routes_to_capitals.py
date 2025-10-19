import sqlite3
import json
import os
import time
from google.maps import routing_v2
from google.protobuf import field_mask_pb2
from google.protobuf.json_format import MessageToJson
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
DATABASE_NAME = 'hungarian_towns.db'
NEIGHBORS_FILE = 'county_neighbors.json'
API_KEY = os.getenv('ROUTES_API_KEY') # Securely load API key

# --- DATABASE FUNCTIONS ---

def add_commute_columns():
    """Adds columns to the database to store commute data. Idempotent."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("ALTER TABLE towns ADD COLUMN commute_budapest_mins INTEGER")
        cursor.execute("ALTER TABLE towns ADD COLUMN commute_nearest_capital_mins INTEGER")
        cursor.execute("ALTER TABLE towns ADD COLUMN nearest_capital_name TEXT")
        print("Database columns added successfully.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("Database columns already exist. Skipping.")
        else:
            raise e
    conn.commit()
    conn.close()

def get_all_towns_data():
    """Fetches all towns with their coordinates and county from the database."""
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row # Allows accessing columns by name
    cursor = conn.cursor()
    cursor.execute("SELECT name, county, latitude, longitude FROM towns WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
    towns = cursor.fetchall()
    conn.close()
    return [dict(row) for row in towns]

def update_town_in_db(town_name, budapest_mins, nearest_capital_mins, nearest_capital_name):
    """Updates a single town's record with the new commute data."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE towns
        SET commute_budapest_mins = ?,
            commute_nearest_capital_mins = ?,
            nearest_capital_name = ?
        WHERE name = ?
    """, (budapest_mins, nearest_capital_mins, nearest_capital_name, town_name))
    conn.commit()
    conn.close()

# --- DATA PREPARATION ---

def load_county_neighbors():
    """Loads the county neighbors and capital names from the JSON file."""
    with open(NEIGHBORS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_capital_cities_coords(all_towns, county_data):
    """
    Dynamically builds a dictionary of capital city names to their coordinates
    by looking them up in the scraped towns data.
    """
    capital_names = {data['capital'] for data in county_data.values()}
    capital_coords = {}
    for town in all_towns:
        if town['name'] in capital_names:
            capital_coords[town['name']] = {
                "latitude": town['latitude'],
                "longitude": town['longitude']
            }
    print(f"Successfully located coordinates for {len(capital_coords)} capital cities in the database.")
    return capital_coords


# --- GOOGLE ROUTES API FUNCTION ---

def calculate_commute_times(client, origin_coords, destinations):
    """
    Calls the Google Routes API's computeRouteMatrix to get drive times.
    """
    if not destinations:
        return {}
    
    route_origins = [routing_v2.RouteMatrixOrigin(
        waypoint=routing_v2.Waypoint(location=routing_v2.Location(lat_lng=origin_coords))
    )]
    route_destinations = [
        routing_v2.RouteMatrixDestination(
            waypoint=routing_v2.Waypoint(location=routing_v2.Location(lat_lng=dest_coords))
        ) for dest_coords in destinations.values()
    ]
    
    request = routing_v2.ComputeRouteMatrixRequest(
        origins=route_origins,
        destinations=route_destinations,
        travel_mode=routing_v2.RouteTravelMode.DRIVE,
    )

    # Create the FieldMask object to hold our desired paths
    field_mask = field_mask_pb2.FieldMask(paths=["origin_index", "destination_index", "duration", "status"])

    try:
        # *** THE FINAL FIX ***
        # Manually join the paths into a simple comma-separated string.
        # This is the format the x-goog-fieldmask header expects.
        response_stream = client.compute_route_matrix(
            request=request,
            metadata=[("x-goog-fieldmask", ",".join(field_mask.paths))],
        )
    except Exception as e:
        print(f"  ERROR: API call failed: {e}")
        return None

    results = {}
    destination_names = list(destinations.keys())
    for element in response_stream:
        if element.status.code == 0:
            duration_seconds = element.duration.seconds
            commute_mins = round(duration_seconds / 60)
            dest_name = destination_names[element.destination_index]
            results[dest_name] = commute_mins
        else:
            dest_name = destination_names[element.destination_index]
            print(f"  WARNING: Could not find route to {dest_name}. Status: {element.status.message} (Code: {element.status.code})")
            results[dest_name] = None
    return results


# --- MAIN EXECUTION ---

def main():
    if not API_KEY:
        print("ERROR: GOOGLE_API_KEY environment variable not set. Exiting.")
        return

    # Initialize the API client
    routes_client = routing_v2.RoutesClient(
        client_options={"api_key": API_KEY}
    )
    
    # 1. Prepare database and load data
    add_commute_columns()
    county_data = load_county_neighbors()
    all_towns = get_all_towns_data()
    capital_coords_map = get_capital_cities_coords(all_towns, county_data)

    print("\nStarting commute time enrichment process...")
    total_towns = len(all_towns)

    # 2. Loop through each town and process
    for i, town in enumerate(all_towns[500:1000]):
        print(f"({i+1}/{total_towns}) Processing: {town['name']}...")
        
        # Normalize county name (e.g., "Fejér vármegye" -> "Fejér")
        town_county = town['county'].replace(' vármegye', '')
        if town_county not in county_data:
            print(f"  WARNING: County '{town['county']}' not found in relationships JSON. Skipping.")
            continue

        # 3. Build the list of destinations for this specific town
        destinations_to_check = {}
        relevant_county_names = [town_county] + county_data[town_county].get('neighbors', [])
        
        for county_name in relevant_county_names:
            capital_name = county_data.get(county_name, {}).get('capital')
            if capital_name and capital_name in capital_coords_map:
                destinations_to_check[capital_name] = capital_coords_map[capital_name]
        
        # Always add Budapest
        if "Budapest" in capital_coords_map:
            destinations_to_check["Budapest"] = capital_coords_map["Budapest"]

        # 4. Make the API call
        origin_coords = {"latitude": town['latitude'], "longitude": town['longitude']}
        commute_results = calculate_commute_times(routes_client, origin_coords, destinations_to_check)
        
        if commute_results is None:
            print(f"  Skipping database update for {town['name']} due to API error.")
            continue

        # 5. Process results and update the database
        budapest_mins = commute_results.get("Budapest")
        
        capital_commutes = {
            name: mins for name, mins in commute_results.items() 
            if name != "Budapest" and mins is not None
        }

        if capital_commutes:
            nearest_capital_name = min(capital_commutes, key=capital_commutes.get)
            nearest_capital_mins = capital_commutes[nearest_capital_name]
            
            update_town_in_db(
                town['name'],
                budapest_mins,
                nearest_capital_mins,
                nearest_capital_name
            )
            print(f"  -> Budapest: {budapest_mins} min. Nearest capital: {nearest_capital_name} ({nearest_capital_mins} min). DB updated.")
        else:
            print(f"  -> Could not determine nearest capital for {town['name']}.")
            # Still update with Budapest time if available
            update_town_in_db(town['name'], budapest_mins, None, None)

        # A small delay to respect API rate limits and avoid overwhelming the service
        time.sleep(0.05) 

    print("\nEnrichment process complete!")

if __name__ == "__main__":
    main()