import sqlite3
import sys

DATABASE_NAME = 'hungarian_towns.db'

def correct_coordinate(coord):
    """
    Applies the heuristic: if a coordinate is a large number,
    it converts it from (e.g.) 47123456.0 to 47.123456.
    """
    # If the coordinate is None or already looks valid, don't touch it.
    if coord is None or (abs(coord) < 180):
        return coord

    try:
        # Convert the large float to an integer string to remove the trailing '.0'
        coord_str = str(int(coord))

        # Re-insert the decimal point after the first two digits
        if len(coord_str) > 2:
            corrected_str = coord_str[:2] + '.' + coord_str[2:]
            return float(corrected_str)
        else:
            # This case shouldn't happen, but as a safeguard:
            return float(coord)

    except (ValueError, TypeError):
        # If any conversion fails, return the original value
        return coord

def main():
    """
    Connects to the database, reads all town coordinates, fixes them
    in memory, and writes the corrected values back to the database.
    """
    print(f"Connecting to database: {DATABASE_NAME}")
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        # Fetch all rows with potentially broken coordinates
        cursor.execute("SELECT name, latitude, longitude FROM towns WHERE latitude > 180 OR longitude > 180")
        rows_to_fix = cursor.fetchall()

        if not rows_to_fix:
            print("No invalid coordinates found. Your database looks clean!")
            conn.close()
            return

        print(f"Found {len(rows_to_fix)} rows with invalid coordinates. Starting correction process...")

        updates = []
        for name, lat, lon in rows_to_fix:
            corrected_lat = correct_coordinate(lat)
            corrected_lon = correct_coordinate(lon)
            updates.append((corrected_lat, corrected_lon, name))

        # Use executemany for an efficient batch update
        cursor.executemany("UPDATE towns SET latitude = ?, longitude = ? WHERE name = ?", updates)

        conn.commit()
        print(f"Successfully corrected and updated {cursor.rowcount} rows in the database.")

    except sqlite3.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    main()