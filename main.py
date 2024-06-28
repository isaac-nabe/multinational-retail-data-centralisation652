# Imports the necessary classes from our other scripts.
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    # Initializes instances of our DatabaseConnector, DataExtractor, and DataCleaning classes.
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaning = DataCleaning()

    """M2:T3 - Extract and clean user data"""

    # Lists the tables in the RDS database and prints them. Helps us verify which tables are available.
    tables = db_connector.list_db_tables()
    print(f"Tables in the RDS database: {tables}")

    # Extracts data from the specified table ('legacy_users' table name containing user data) and stores it in a DataFrame.
    user_data_df = data_extractor.read_rds_table(db_connector, 'legacy_users')
    print("User data extracted successfully")
    print("Extracted DataFrame head:\n", user_data_df.head())  # Print the first few rows for debugging
    print("Extracted User DataFrame info:\n")
    user_data_df.info()  # Print DataFrame info to inspect data types and missing values

    # Cleans the extracted user data & prints success message
    cleaned_user_data_df = data_cleaning.clean_user_data(user_data_df)
    print("User data cleaned successfully")
    print("Cleaned User DataFrame head:\n", cleaned_user_data_df.head())
    print("Cleaned User DataFrame info:\n")
    cleaned_user_data_df.info()  # Print DataFrame info to inspect data types and missing values

    # Save the cleaned User DataFrame to a CSV file for inspection
    cleaned_user_data_df.to_csv("cleaned_user_data.csv", index=False)

    # Uploads the cleaned user data to the dim_users table in our local sales_data database and prints a success message.
    db_connector.upload_to_db(cleaned_user_data_df, 'dim_users')
    print("Cleaned user data uploaded successfully to 'dim_users' table")

    """M2:T4 - Extract and clean card details"""
    
    # Step 1: Extract card data from PDF
    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"  # Input the link to the PDF here
    card_data_df = data_extractor.retrieve_pdf_data(pdf_link)
    print("Card data extracted successfully")
    print("Extracted Card DataFrame head:\n", card_data_df.head())  # Print the first few rows for debugging
    print("Extracted Card DataFrame info:\n")
    card_data_df.info()  # Print DataFrame info to inspect data types and missing values

    # Step 2: Clean the extracted card data
    cleaned_card_data_df = data_cleaning.clean_card_data(card_data_df)
    print("Card data cleaned successfully")
    print("Cleaned Card DataFrame head:\n", cleaned_card_data_df.head())
    print("Cleaned Card DataFrame info:\n")
    cleaned_card_data_df.info()  # Print DataFrame info to inspect data types and missing values

    # Save the cleaned Card DataFrame to a CSV file for inspection
    cleaned_card_data_df.to_csv("cleaned_card_data.csv", index=False)

    # Step 3: Upload the cleaned card data to the database
    db_connector.upload_to_db(cleaned_card_data_df, 'dim_card_details')
    print("Cleaned card data uploaded successfully to 'dim_card_details' table")

# Condition ensuring that the main function runs when the script is executed.
if __name__ == "__main__":
    main()
