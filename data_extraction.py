import pandas as pd
import tabula
import requests

class DataExtractor:
    def read_rds_table(self, db_connector, table_name):
        """
        Reads data from a specified table in an RDS database into a Pandas DataFrame.

        :param db_connector: Instance of the DatabaseConnector class.
        :param table_name: Name of the table to read from the database.
        :return: DataFrame containing the data from the specified table.
        """
        # Read database credentials from a YAML file
        creds = db_connector.read_db_creds('db_creds_rds.yaml')
        
        # Initialize a database engine using the credentials
        engine = db_connector.init_db_engine({
            'USER': creds['RDS_USER'],
            'PASSWORD': creds['RDS_PASSWORD'],
            'HOST': creds['RDS_HOST'],
            'PORT': creds['RDS_PORT'],
            'DATABASE': creds['RDS_DATABASE']
        })
        
        # SQL query to select all data from the specified table
        query = f"SELECT * FROM {table_name}"
        
        # Execute the query and read the data into a Pandas DataFrame
        df = pd.read_sql(query, engine)
        
        # Return the DataFrame
        return df

    def retrieve_pdf_data(self, pdf_link):
        """
        Extracts data from a PDF document and returns it as a Pandas DataFrame.

        :param pdf_link: URL link to the PDF file.
        :return: DataFrame containing the extracted data.
        """
        # Use Tabula to read the PDF and extract all pages into a list of DataFrames
        df_list = tabula.read_pdf(pdf_link, pages='all')
        
        # Concatenate all DataFrames in the list into a single DataFrame
        df = pd.concat(df_list, ignore_index=True)
        
        # Return the DataFrame
        return df

    def list_number_of_stores(self, url, header):
        """
        Retrieves the total number of stores from the API.

        :param url: API endpoint to get the number of stores.
        :param header: Dictionary containing API headers.
        :return: Total number of stores.
        """
        # Send a GET request to the API endpoint with the provided headers
        response = requests.get(url, headers=header)
        
        # Parse the JSON response
        data = response.json()
        
        # Check if the response is successful and contains 'number_stores' key
        if response.status_code == 200 and 'number_stores' in data:
            # Return the total number of stores
            return data['number_stores']
        else:
            # Raise an error if the 'number_stores' key is not found in the response
            raise KeyError(f"'number_stores' key not found in response: {data}")

    def retrieve_stores_data(self, url, header, number_of_stores):
        """
        Retrieves data for all stores from the API.

        :param url: API endpoint to get store details.
        :param header: Dictionary containing API headers.
        :param number_of_stores: Total number of stores to retrieve.
        :return: DataFrame containing details of all stores.
        """
        # Initialize an empty list to hold store data
        stores_data = []
        
        # Loop over the range of store numbers to retrieve data for each store
        for store_number in range(0, number_of_stores):
            # Format the store URL with the current store number
            store_url = url.format(store_number=store_number)
            
            # Send a GET request to the store URL with the provided headers
            response = requests.get(store_url, headers=header)
            
            if response.status_code == 200:
                try:
                    # Parse the JSON response and append it to the stores_data list
                    store_data = response.json()
                    stores_data.append(store_data)
                except ValueError as e:
                    # Print an error message if there is an issue parsing the JSON
                    print(f"Error parsing JSON for store {store_number}: {e}")
            else:
                # Print an error message if the request fails
                print(f"Error fetching data for store {store_number}: {response.status_code}, {response.text}")
                
                # Break the loop if a 404 error is encountered (store not found)
                if response.status_code == 404:
                    break

        # Convert the list of store data into a Pandas DataFrame
        df = pd.DataFrame(stores_data)
        
        # save df as csv
        df.to_csv('sample_df_pre_clean.csv', index=False)

        # Return the DataFrame
        return df

if __name__ == '__main__':
    pass
