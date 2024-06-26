# Imports the necessary classes from our other scripts.
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    # Initializes instances of our DatabaseConnector, DataExtractor, and DataCleaning classes.
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaning = DataCleaning()

    # Lists the tables in the RDS database and prints them. Helps us verify which tables are available.
    tables = db_connector.list_db_tables()
    print(f"Tables in the RDS database: {tables}")

    # Extracts data from the specified table ('legacy_users' table name containing user data) and stores it in a DataFrame.
    # Also prints a success message.
    user_data_df = data_extractor.read_rds_table(db_connector, 'legacy_users')
    print("User data extracted successfully")
    print("Extracted DataFrame head:\n", user_data_df.head())  # Print the first few rows for debugging

    # Cleans the extracted user data & prints success message
    cleaned_user_data_df = data_cleaning.clean_user_data(user_data_df)
    print("User data cleaned successfully")

    # Uploads the cleaned user data to the dim_users table in our local sales_data database and prints a success message.
    db_connector.upload_to_db(cleaned_user_data_df, 'dim_users')
    print("Cleaned user data uploaded successfully to 'dim_users' table")

# Condition ensuring that the main function runs when the script is executed.
if __name__ == "__main__":
    main()
    

# The following commented code is useful for checking the table names in your RDS database.
"""
from database_utils import DatabaseConnector

def main():
    # Initialize database connector
    db_connector = DatabaseConnector()

    # List tables in the RDS database
    tables = db_connector.list_db_tables()
    print(f"Tables in the RDS database: {tables}")

if __name__ == "__main__":
    main()
"""
