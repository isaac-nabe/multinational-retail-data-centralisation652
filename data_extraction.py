import pandas as pd
import tabula
import requests
import boto3

class DataExtractor:
    def read_rds_table(self, db_connector, table_name: str) -> pd.DataFrame:
        """
        Reads data from a specified table in an RDS database into a Pandas DataFrame.

        :param db_connector: Instance of the DatabaseConnector class.
        :param table_name: Name of the table to read from the database.
        :return: DataFrame containing the data from the specified table.
        """
        creds, creds_type = db_connector.read_db_creds('db_creds_rds.yaml')
        engine = db_connector.init_db_engine(creds, creds_type)
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, engine)

    def retrieve_pdf_data(self, pdf_link: str) -> pd.DataFrame:
        """
        Extracts data from a PDF document and returns it as a Pandas DataFrame.

        :param pdf_link: URL link to the PDF file.
        :return: DataFrame containing the extracted data.
        """
        df_list = tabula.read_pdf(pdf_link, pages='all')
        return pd.concat(df_list, ignore_index=True)

    def list_number_of_stores(self, url: str, header: dict) -> int:
        """
        Retrieves the total number of stores from the API.

        :param url: API endpoint to get the number of stores.
        :param header: Dictionary containing API headers.
        :return: Total number of stores.
        """
        response = requests.get(url, headers=header)
        data = response.json()
        if response.status_code == 200 and 'number_stores' in data:
            return data['number_stores']
        else:
            raise KeyError(f"'number_stores' key not found in response: {data}")

    def retrieve_stores_data(self, url: str, header: dict, number_of_stores: int) -> pd.DataFrame:
        """
        Retrieves data for all stores from the API.

        :param url: API endpoint to get store details.
        :param header: Dictionary containing API headers.
        :param number_of_stores: Total number of stores to retrieve.
        :return: DataFrame containing details of all stores.
        """
        stores_data = []
        for store_number in range(0, number_of_stores):
            store_url = url.format(store_number=store_number)
            response = requests.get(store_url, headers=header)
            if response.status_code == 200:
                try:
                    store_data = response.json()
                    stores_data.append(store_data)
                except ValueError as e:
                    print(f"Error parsing JSON for store {store_number}: {e}")
            else:
                print(f"Error fetching data for store {store_number}: {response.status_code}, {response.text}")
                if response.status_code == 404:
                    break
        return pd.DataFrame(stores_data)

    def extract_from_s3(self, s3_products_address: str) -> pd.DataFrame:
        """
        Downloads and extracts a CSV file from an S3 bucket.

        :param s3_products_address: S3 address of the file.
        :return: DataFrame containing the data from the CSV file.
        """
        bucket_name = s3_products_address.split('/')[2]
        file_key = '/'.join(s3_products_address.split('/')[3:])
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, file_key, 'products.csv')
        return pd.read_csv('products.csv')

    def extract_json_from_url(self, s3_sale_dates_address: str) -> pd.DataFrame:
        """
        Extracts a JSON file from the specified URL and returns it as a DataFrame.

        :param url: URL to the JSON file.
        :return: DataFrame containing the data from the JSON file.
        """
        response = requests.get(s3_sale_dates_address)
        data = response.json()
        return pd.DataFrame(data)


if __name__ == '__main__':
    pass
