import pandas as pd
import psycopg2
import logging
import os

from contextlib import closing
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def connect_to_database():
    """Connect to the PostgreSQL database."""
    # Get database connection info from environment variables or use default values
    host = os.environ.get("DB_HOST", "localhost")
    database = os.environ.get("DB_NAME", "postgres")
    user = os.environ.get("DB_USER", "postgres")
    password = os.environ.get("DB_PASSWORD", "admin")
    port = os.environ.get("DB_PORT", 5433)

    try:
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        logging.info("Database connection successful!")
        return connection
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        return None

def safe_int(value):
    """Convert a value to integer, return None if invalid."""
    try:
        return int(value)
    except (ValueError, TypeError):
        logging.warning(f"Invalid integer value: {value}")
        return None

def safe_date(value):
    """Convert a value to a valid date string, return None if invalid."""
    try:
        return datetime.strptime(value, "%d/%m/%Y").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        logging.warning(f"Invalid date value: {value}")
        return None

def safe_text(value):
    """Convert a text value to a clean string, return None if invalid."""
    return str(value).strip() if pd.notnull(value) else None

def load_csv_to_db(connection, csv_file):
    """Load data from a CSV file into the PostgreSQL database."""
    if not os.path.exists(csv_file):
        logging.error(f"CSV file not found: {csv_file}")
        return

    try:
        # Load the CSV file with comma as the delimiter
        data = pd.read_csv(csv_file, delimiter=",", encoding="utf-8")
        logging.info("CSV file loaded successfully.")

        # Validate required columns
        required_columns = [
            "beneficiaire_age", "beneficiaire_genre", "organisme",
            "date_recours_pass_sport", "federation", "region",
            "departement", "commune"
        ]
        if not all(column in data.columns for column in required_columns):
            missing_columns = [col for col in required_columns if col not in data.columns]
            logging.error(f"CSV file is missing required columns: {missing_columns}")
            return

        with closing(connection.cursor()) as cursor:
            # Create table if not exists
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS pass_sport_records (
                id SERIAL PRIMARY KEY,
                beneficiaire_age INT CHECK (beneficiaire_age > 0),
                beneficiaire_genre TEXT CHECK (beneficiaire_genre IN ('M', 'F')),
                organisme TEXT,
                date_recours_pass_sport DATE,
                federation TEXT,
                region TEXT,
                departement TEXT,
                commune TEXT
            );
            """)
            logging.info("Table 'pass_sport_records' created or already exists.")

            # Prepare data for insertion
            rows_to_insert = []
            for index, row in data.iterrows():
                try:
                    beneficiaire_age = safe_int(row['beneficiaire_age'])
                    beneficiaire_genre = row['beneficiaire_genre'].strip() if row['beneficiaire_genre'] in ['M', 'F'] else None
                    organisme = safe_text(row['organisme'])
                    date_recours = safe_date(row['date_recours_pass_sport'])
                    federation = safe_text(row['federation'])
                    region = safe_text(row['region'])
                    departement = safe_text(row['departement'])
                    commune = safe_text(row['commune'])

                    if beneficiaire_age and beneficiaire_genre and organisme and date_recours:
                        rows_to_insert.append((
                            beneficiaire_age, beneficiaire_genre, organisme, date_recours,
                            federation, region, departement, commune
                        ))
                    else:
                        logging.warning(f"Skipping row {index + 1} due to missing or invalid values: {row.to_dict()}")

                except Exception as e:
                    logging.error(f"Error processing row {index + 1}: {e}")
                    logging.error(f"Row data: {row.to_dict()}")

            # Insert data in batches
            if rows_to_insert:
                cursor.executemany("""
                INSERT INTO pass_sport_records (
                    beneficiaire_age, beneficiaire_genre, organisme, date_recours_pass_sport,
                    federation, region, departement, commune
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """, rows_to_insert)
                connection.commit()
                logging.info(f"Inserted {len(rows_to_insert)} rows successfully.")
            else:
                logging.warning("No valid rows to insert.")

    except Exception as e:
        logging.error(f"An error occurred while processing the CSV: {e}")
        connection.rollback()  # Rollback in case of error
    finally:
        if connection:
            connection.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    csv_file = "../data_AJ.csv"  # Replace with your CSV file path
    connection = connect_to_database()

    if connection:
        load_csv_to_db(connection, csv_file)
