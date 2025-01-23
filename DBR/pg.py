import pandas as pd
import psycopg2
from psycopg2 import sql

def connect_to_database():
    host = "localhost"  # Change to your database host
    database = "postgres"  # Replace with your database name
    user = "postgres"  # Replace with your username
    password = "admin"  # Replace with your password
    port = 5433

    try:
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        print("Database connection successful!")
        return connection
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def safe_int(value):
    """Convert a value to integer, return None if invalid."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def load_csv_to_db(connection, csv_file):
    cursor = None
    try:
        # Load the CSV file with semicolon as the delimiter
        data = pd.read_csv(csv_file, delimiter=";")
        print("CSV file loaded successfully.")

        cursor = connection.cursor()

        # Create table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS colleges (
            rentree_scolaire INT,
            code_region_academique INT,
            region_academique TEXT,
            code_academie INT,
            academie TEXT,
            code_departement INT,
            departement TEXT,
            code_postal INT,
            commune TEXT,
            uai TEXT PRIMARY KEY,
            denomination_principale TEXT,
            patronyme TEXT,
            secteur TEXT,
            rep INT,
            rep_plus INT,
            nombre_eleves_total INT,
            nombre_eleves_total_hors_segpa_ulis INT,
            nombre_eleves_segpa INT,
            nombre_eleves_ulis INT
        );
        """)
        print("Table 'colleges' created or already exists.")

        # Insert data into the table
        for _, row in data.iterrows():
            cursor.execute("""
            INSERT INTO colleges (
                rentree_scolaire, code_region_academique, region_academique, code_academie, academie, 
                code_departement, departement, code_postal, commune, uai, denomination_principale, 
                patronyme, secteur, rep, rep_plus, nombre_eleves_total, 
                nombre_eleves_total_hors_segpa_ulis, nombre_eleves_segpa, nombre_eleves_ulis
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (uai) DO NOTHING;
            """, (
                row['Rentrée scolaire'],
                safe_int(row['Code région académique']),
                row['Région académique'],
                safe_int(row['Code académie']),
                row['Académie'],
                safe_int(row['Code département']),
                row['Département'],
                safe_int(row['Code postal']),
                row['Commune'],
                row['UAI'],
                row['Dénomination principale'],
                row['Patronyme'],
                row['Secteur'],
                safe_int(row['REP']),
                safe_int(row['REP +']),
                safe_int(row['Nombre d\'élèves total']),
                safe_int(row['Nombre d\'élèves total hors Segpa hors ULIS']),
                safe_int(row['Nombre d\'élèves total Segpa']),
                safe_int(row['Nombre d\'élèves total ULIS'])
            ))
        print("Data inserted successfully.")

        # Commit changes
        connection.commit()
        print("Changes committed.")
    except Exception as e:
        print(f"An error occurred while processing the CSV: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()

if __name__ == "__main__":
    csv_file = "../data_college.csv"  # Replace with your CSV file path
    connection = connect_to_database()

    if connection:
        try:
            load_csv_to_db(connection, csv_file)
        finally:
            connection.close()
            print("Database connection closed.")
