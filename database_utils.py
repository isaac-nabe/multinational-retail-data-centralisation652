import yaml  # Imports the YAML library to handle reading YAML files.
from sqlalchemy import create_engine, text  # Imports SQLAlchemy for database interactions and text for SQL expressions.



class DatabaseConnector:
    def read_db_creds(self, file_name):
        """
        Reads the database credentials from a YAML file and returns them as a dictionary.
        This allows you to securely handle sensitive information.

        :param file_name: The name of the YAML file containing the database credentials.
        :return: A dictionary containing the database credentials.
        """
        with open(file_name, 'r') as file:  # Opens the specified YAML file in read mode.
            creds = yaml.safe_load(file)  # Reads the YAML file and loads its content into a dictionary named creds.
        return creds  # Returns the dictionary containing the database credentials.


    def init_db_engine(self, creds):
        """
        Initializes and returns a SQLAlchemy database engine using the provided credentials.
        This engine will be used to interact with the database.

        :param creds: A dictionary containing the database credentials.
        :return: The initialized SQLAlchemy database engine.
        """
        # Uses the credentials to create a database engine.
        engine = create_engine(f"postgresql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}")
        return engine  # Returns the database engine.


    def list_db_tables(self):
        """
        Lists all tables in the database.

        :return: A list of all table names in the database.
        """
        # Reads the credentials for the RDS database.
        creds = self.read_db_creds('db_creds_rds.yaml')
        # Initializes the database engine with the RDS credentials.
        engine = self.init_db_engine({
            'USER': creds['RDS_USER'],
            'PASSWORD': creds['RDS_PASSWORD'],
            'HOST': creds['RDS_HOST'],
            'PORT': creds['RDS_PORT'],
            'DATABASE': creds['RDS_DATABASE']
        })
        with engine.connect() as connection:  # Opens a connection to the database.
            # Executes a SQL query to list all table names in the public schema.
            result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public';"))
            # Creates a list of table names from the query result.
            tables = [row[0] for row in result]
        return tables  # Returns the list of table names.


    def upload_to_db(self, data_frame, table_name):
        """
        Uploads a Pandas DataFrame to a specified table in the database.

        :param data_frame: The Pandas DataFrame to upload.
        :param table_name: The name of the table to upload the data to.
        """
        # Reads the credentials for the local writable database.
        creds = self.read_db_creds('db_creds_local.yaml')
        # Initializes the database engine with local credentials.
        engine = self.init_db_engine({
            'USER': creds['LOCAL_USER'],
            'PASSWORD': creds['LOCAL_PASSWORD'],
            'HOST': creds['LOCAL_HOST'],
            'PORT': creds['LOCAL_PORT'],
            'DATABASE': creds['LOCAL_DATABASE']
        })
        # Uploads the DataFrame to the specified table in the database, replacing the table if it exists.
        data_frame.to_sql(table_name, engine, if_exists='replace', index=False)



if __name__ == '__main__':
    pass  # Pass statement for when the script is run directly
