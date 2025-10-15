import sqlite3
import json

DATABASE_NAME = 'hungarian_towns.db'
NEIGHBORS_FILE = 'county_neighbors.json'
BUDAPEST_COORDS = {"latitude": 47.4983, "longitude": 19.0408}

def load_county_data():
    """Loads the county neighbors and capitals from the JSON file."""
    with open(NEIGHBORS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_towns_from_db():
    """Fetches all towns with their coordinates and county from the database."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    # Fetch only towns that have coordinates
    cursor.execute("SELECT name, county, latitude, longitude FROM towns WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
    towns = cursor.fetchall()
    conn.close()
    return towns

def generate_commute_plan(towns, county_data):
    """
    Generates a plan of which routes to calculate for each town.
    This doesn't call the API, just prepares the list of origins and destinations.
    """
    commute_plan = {}
    
    for town_name, town_county_raw, town_lat, town_lon in towns:
        # The scraper might have stored county as "Csongrád-Csanád" or "Csongrád". Let's try to normalize.
        town_county = town_county_raw
        if town_county not in county_data:
            # Handle cases like "Borsod-Abaúj-Zemplén" vs "Borsod-Abaúj-Zemplén vármegye"
            # Or "Csongrád" vs "Csongrád-Csanád"
            if town_county == "Csongrád":
                town_county = "Csongrád-Csanád"
            else:
                print(f"Warning: County '{town_county}' for town '{town_name}' not found in JSON. Skipping.")
                continue

        town_origin = {"latitude": town_lat, "longitude": town_lon}
        destinations_to_check = {}

        # 1. Add the town's own county capital
        own_capital_info = county_data[town_county]
        destinations_to_check[own_capital_info['capital']] = own_capital_info['capital_coords']

        # 2. Add the capitals of all neighboring counties
        for neighbor_county_name in own_capital_info['neighbors']:
            neighbor_capital_info = county_data[neighbor_county_name]
            destinations_to_check[neighbor_capital_info['capital']] = neighbor_capital_info['capital_coords']

        # 3. Always add Budapest
        destinations_to_check["Budapest"] = BUDAPEST_COORDS
        
        commute_plan[town_name] = {
            "origin": town_origin,
            "destinations": destinations_to_check
        }
        
    return commute_plan


def main():
    county_data = load_county_data()
    all_towns = get_towns_from_db()
    plan = generate_commute_plan(all_towns, county_data)

    print(f"Generated a commute calculation plan for {len(plan)} towns.")
    print("-" * 50)

    # --- THIS IS WHERE YOU WOULD CALL THE GOOGLE ROUTES API ---
    # You would iterate through the 'plan' dictionary and for each town,
    # make one or more API calls with its origin and list of destinations.
    
    # Example of what the loop would look like:
    # for town_name, data in plan.items():
    #     origin_coords = data['origin']
    #     destination_coords_list = list(data['destinations'].values())
    #
    #     # A single Google Routes API call can handle one origin and multiple destinations
    #     # This is highly cost-effective!
    #     results = call_google_routes_api(origin_coords, destination_coords_list)
    #
    #     # Find the minimum commute time from the results
    #     min_commute_time = ...
    #     nearest_capital = ...
    #
    #     # Get commute time to Budapest specifically
    #     commute_to_budapest = ...
    #
    #     # Update your database with this new information
    #     update_database(town_name, min_commute_time, nearest_capital, commute_to_budapest)

    # For now, let's just print the plan for a few towns to see how it works:
    print("Example plan for a few towns:")
    for i, (town_name, data) in enumerate(plan.items()):
        if i >= 5: break
        print(f"\nTown: {town_name} ({data['origin']})")
        print("  Destinations to calculate commute time for:")
        for dest_name, dest_coords in data['destinations'].items():
            print(f"    - {dest_name} ({dest_coords})")


if __name__ == "__main__":
    main()