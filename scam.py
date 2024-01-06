import requests
import psycopg2
from io import StringIO

# PostgreSQL connection parameters
db_params = {
    'host': '15.207.221.153',
    'database': 'domain_db',
    'user': 'bewin',
    'password': 'root',
    'port': '5433'
}

# URL of the file containing names
file_url = 'https://raw.githubusercontent.com/ShadowWhisperer/BlockLists/master/RAW/Scam'

# Function to fetch names from the file
def fetch_names_from_file(url):
    print(f"Fetching names from {url}")
    response = requests.get(url)
    names = response.text.split('\n')
    return names

# Function to check if a name is already present in the table
def is_name_present(cursor, name):
    cursor.execute('SELECT COUNT(*) FROM blocklist WHERE domain = %s', (name,))
    count = cursor.fetchone()[0]
    return count > 0

# Function to insert names into PostgreSQL table
def insert_names_into_table(names, db_params):
    print("Connecting to PostgreSQL database...")
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Prepare the names for insertion
    values = [(name,) for name in names if name]

    print(f"Inserting names into the table 'blocklist' with category 'scam' and source 'github(ShadowWhisperer)'...")
    for value in values:
        name = value[0]
        if not is_name_present(cursor, name):
            print(f"Inserting {name}")
            cursor.execute('INSERT INTO blocklist (domain, category, source) VALUES (%s, %s, %s)', (name, 'scam', 'github(ShadowWhisperer)'))
            conn.commit()
        else:
            print(f"Skipping duplicate: {name}")

    cursor.close()
    conn.close()
    print("Connection closed.")

if __name__ == '__main__':
    print("Script started.")
    names = fetch_names_from_file(file_url)
    print(f"Fetched {len(names)} names from the file.")
    insert_names_into_table(names, db_params)
    print("Script completed.")