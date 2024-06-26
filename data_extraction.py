import pandas as pd
"""
import pandas as pd: Imports the Pandas library for data manipulation.
"""

# define DataExtractor class
class DataExtractor:
    def read_rds_table(self, db_connector, table_name):
        """
        Create method to read data from a specified table into a Pandas DataFrame from the list of all tables 
        created by the "list_db_tables()" method in database_utils.py.
        This helps in extracting data from available tables.

        # creds = db_connector.read_db_creds('db_creds_rds.yaml'): 
        # engine = db_connector.init_db_engine(): Calls the init_db_engine method of the DatabaseConnector class to get the database engine.
        # query = f"SELECT * FROM {table_name}": Constructs an SQL query to select all data from the specified table.
        # df = pd.read_sql(query, engine): Executes the query and stores the result in a Pandas DataFrame.
        # return df: Returns the DataFrame.
        """
        # Read the RDS credentials
        creds = db_connector.read_db_creds('db_creds_rds.yaml')
        # Initialize the database engine with the RDS credentials
        engine = db_connector.init_db_engine({
            'USER': creds['RDS_USER'],
            'PASSWORD': creds['RDS_PASSWORD'],
            'HOST': creds['RDS_HOST'],
            'PORT': creds['RDS_PORT'],
            'DATABASE': creds['RDS_DATABASE']
        })
        # Construct the query
        query = f"SELECT * FROM {table_name}"
        # Execute the query and store the result in a Pandas DataFrame
        df = pd.read_sql(query, engine)
        return df

if __name__ == '__main__':
    pass
