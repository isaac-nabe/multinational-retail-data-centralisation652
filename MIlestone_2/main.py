from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from config import pdf_link, API_KEY, number_of_stores_url, store_url_template, s3_products_address, s3_sale_dates_address

def extract_and_clean_rds_data(db_connector, data_extractor, table_name, clean_func, output_csv, db_table_name):
    """
    Extracts and cleans data from a specified table, saves it to a CSV file, and uploads it to the database.

    :param db_connector: Instance of DatabaseConnector for database operations.
    :param data_extractor: Instance of DataExtractor for data extraction.
    :param table_name: Name of the table to extract data from.
    :param clean_func: Cleaning function from the DataCleaning class.
    :param output_csv: Name of the output CSV file.
    :param db_table_name: Name of the table to upload cleaned data to.
    """
    # Read the data from the specified table
    data_df = data_extractor.read_rds_table(db_connector, table_name)
    print(f"{table_name} data extracted successfully")
    print("Extracted DataFrame head:\n", data_df.head())
    data_df.info()

    # Clean the extracted data
    cleaned_data_df = clean_func(data_df)
    print(f"{table_name} data cleaned successfully")
    print("Cleaned DataFrame head:\n", cleaned_data_df.head())
    cleaned_data_df.info()

    # Save the cleaned data to a CSV file
    cleaned_data_df.to_csv(output_csv, index=False)

    # Upload the cleaned data to the specified table
    db_connector.upload_to_db(cleaned_data_df, db_table_name)
    print(f"Cleaned {table_name} data uploaded successfully to '{db_table_name}' table")

def main():
    """
    Main function to orchestrate the ETL process for user data, card data, store data, product data, orders data, and date events data.
    """
    # Initialize the necessary classes
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaning = DataCleaning()

    # Extract and clean user data
    extract_and_clean_rds_data(db_connector, data_extractor, 'legacy_users', data_cleaning.clean_user_data, "cleaned_user_data.csv", 'dim_users')

    # Extract and clean card details from PDF
    card_data_df = data_extractor.retrieve_pdf_data(pdf_link)
    print("Card data extracted successfully")
    print("Extracted Card DataFrame head:\n", card_data_df.head())
    card_data_df.info()

    cleaned_card_data_df = data_cleaning.clean_card_data(card_data_df)
    print("Card data cleaned successfully")
    print("Cleaned Card DataFrame head:\n", cleaned_card_data_df.head())
    cleaned_card_data_df.info()
    cleaned_card_data_df.to_csv("cleaned_card_data.csv", index=False)
    db_connector.upload_to_db(cleaned_card_data_df, 'dim_card_details')
    print("Cleaned card data uploaded successfully to 'dim_card_details' table")

    # Extract and clean store details
    headers = {"x-api-key": API_KEY}
    number_of_stores = data_extractor.list_number_of_stores(number_of_stores_url, headers)
    print(f"Number of stores: {number_of_stores}")
    stores_df = data_extractor.retrieve_stores_data(store_url_template, headers, number_of_stores)
    if not stores_df.empty:
        print("Stores data extracted successfully")
        stores_df.info()
        cleaned_stores_df = data_cleaning.clean_store_data(stores_df)
        print("Store data cleaned successfully")
        cleaned_stores_df.info()
        cleaned_stores_df.to_csv("cleaned_stores_data.csv", index=False)
        db_connector.upload_to_db(cleaned_stores_df, 'dim_store_details')
        print("Cleaned store data uploaded successfully to 'dim_store_details' table")
    else:
        print("Failed to create DataFrame from stores data.")

    # Extract and clean product details
    product_data_df = data_extractor.extract_from_s3(s3_products_address)
    print("Product data extracted successfully")
    print("Extracted Product DataFrame head:\n", product_data_df.head())
    product_data_df.info()
    converted_product_weights_df = data_cleaning.clean_product_data(product_data_df)
    print("Converted product weights successfully")
    print("Converted Product DataFrame head:\n", converted_product_weights_df.head())
    converted_product_weights_df.info()
    converted_product_weights_df.to_csv("cleaned_product_data.csv", index=False)
    db_connector.upload_to_db(converted_product_weights_df, 'dim_products')
    print("Cleaned product data uploaded successfully to 'dim_products' table")

    # Extract and clean orders data
    extract_and_clean_rds_data(db_connector, data_extractor, 'orders_table', data_cleaning.clean_orders_data, "cleaned_orders_data.csv", 'orders_table')

    # Extract and clean date events data
    date_events_df = data_extractor.extract_json_from_url(s3_sale_dates_address)
    cleaned_date_events_df = data_cleaning.clean_date_events_data(date_events_df)
    cleaned_date_events_df.to_csv("cleaned_date_events_df.csv", index=False)
    db_connector.upload_to_db(cleaned_date_events_df, 'dim_date_times')
    print("Cleaned date events data uploaded successfully to 'dim_date_times' table")

if __name__ == "__main__":
    main()
