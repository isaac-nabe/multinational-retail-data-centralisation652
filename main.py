from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from config import API_KEY


def main():
    """
    Main function to orchestrate the ETL process for user data, card data, and store data.

    Steps:
    1. Initialize instances of DatabaseConnector, DataExtractor, and DataCleaning classes.
    2. Extract and clean user data from the RDS database.
    3. Extract and clean card data from a PDF file.
    4. Extract and clean store data from an API.
    5. Upload the cleaned data to the local PostgreSQL database.
    """

    # Initialize the necessary classes
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaning = DataCleaning()

    """M2:T3 - Extract and clean user data"""
    # List all tables in the RDS database
    tables = db_connector.list_db_tables()
    print(f"Tables in the RDS database: {tables}")

    # Read the user data from the 'legacy_users' table
    user_data_df = data_extractor.read_rds_table(db_connector, 'legacy_users')
    print("User data extracted successfully")
    print("Extracted DataFrame head:\n", user_data_df.head())
    user_data_df.info()

    # Clean the extracted user data
    cleaned_user_data_df = data_cleaning.clean_user_data(user_data_df)
    print("User data cleaned successfully")
    print("Cleaned User DataFrame head:\n", cleaned_user_data_df.head())
    cleaned_user_data_df.info()

    # Save the cleaned user data to a CSV file
    cleaned_user_data_df.to_csv("cleaned_user_data.csv", index=False)

    # Upload the cleaned user data to the 'dim_users' table
    db_connector.upload_to_db(cleaned_user_data_df, 'dim_users')
    print("Cleaned user data uploaded successfully to 'dim_users' table")

    """M2:T4 - Extract and clean card details"""
    # Define the link to the PDF file containing card data
    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

    # Extract card data from the PDF file
    card_data_df = data_extractor.retrieve_pdf_data(pdf_link)
    print("Card data extracted successfully")
    print("Extracted Card DataFrame head:\n", card_data_df.head())
    card_data_df.info()

    # Clean the extracted card data
    cleaned_card_data_df = data_cleaning.clean_card_data(card_data_df)
    print("Card data cleaned successfully")
    print("Cleaned Card DataFrame head:\n", cleaned_card_data_df.head())
    cleaned_card_data_df.info()

    # Save the cleaned card data to a CSV file
    cleaned_card_data_df.to_csv("cleaned_card_data.csv", index=False)

    # Upload the cleaned card data to the 'dim_card_details' table
    db_connector.upload_to_db(cleaned_card_data_df, 'dim_card_details')
    print("Cleaned card data uploaded successfully to 'dim_card_details' table")

    """M2:T5 - Extract and clean store details"""
    # Define the API key and headers for the API request
    # NOTE hide this key inside a config.py or config.yaml so that it's not hard coded 
    headers = {"x-api-key": API_KEY} # changed to API_KEY because it's already imported above

    # Define the URL to get the number of stores
    number_of_stores_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"

    # Retrieve the total number of stores
    number_of_stores = data_extractor.list_number_of_stores(number_of_stores_url, headers)
    print(f"Number of stores: {number_of_stores}")

    # Define the URL template to get store details
    store_url_template = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"

    # Retrieve data for all stores
    stores_df = data_extractor.retrieve_stores_data(store_url_template, headers, number_of_stores)
    if not stores_df.empty:
        print("Stores data extracted successfully")
        stores_df.info()

        # Clean the extracted store data
        cleaned_stores_df = data_cleaning.clean_store_data(stores_df)
        print("Store data cleaned successfully")
        cleaned_stores_df.info()

        # Save the cleaned store data to a CSV file
        cleaned_stores_df.to_csv("cleaned_stores_data.csv", index=False)

        # Upload the cleaned store data to the 'dim_store_details' table
        db_connector.upload_to_db(cleaned_stores_df, 'dim_store_details')
        print("Cleaned store data uploaded successfully to 'dim_store_details' table")
    else:
        print("Failed to create DataFrame from stores data.")

if __name__ == "__main__":
    main()
