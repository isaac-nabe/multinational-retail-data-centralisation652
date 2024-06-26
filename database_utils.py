import yaml
from sqlalchemy import create_engine, text

class DatabaseConnector:
    def read_db_creds(self, file_name):
        """
        Defining a method to read the database credentials from the YAML file and return them as a dictionary.
        This allows you to securely handle sensitive information.

        # with open(file_name, 'r') as file: Opens the specified YAML file in read mode.
        # creds = yaml.safe_load(file): Reads the YAML file and loads its content into a dictionary named creds.
        # return creds: Returns the dictionary containing the database credentials.
        """
        with open(file_name, 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self, creds):
        """
        Creating a method to initialize and return a SQLAlchemy database engine using the provided credentials.
        This engine will be used to interact with the database.

        # engine = create_engine(f"postgresql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}"): Uses the credentials to create a database engine.
        # return engine: Returns the database engine.
        """
        engine = create_engine(f"postgresql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}")
        return engine

    def list_db_tables(self):
        """
        Creates method to list all tables in the database.
        This is so "read_rds_table()" method in data_extraction.py can read data from a specified table into a Pandas DataFrame.
        This helps in identifying available tables so we can extract their data.

        # creds = self.read_db_creds('db_creds_rds.yaml'): Reads the credentials for the RDS database.
        # engine = self.init_db_engine(creds): Initializes the database engine with RDS credentials.
        # with engine.connect() as connection: Opens a connection to the database.
            # result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")): Executes a SQL query to list all table names in the public schema.
            # tables = [row[0] for row in result]: Creates a list of table names from the query result.
        # return tables: Returns the list of table names.
        """
        creds = self.read_db_creds('db_creds_rds.yaml')
        engine = self.init_db_engine({
            'USER': creds['RDS_USER'],
            'PASSWORD': creds['RDS_PASSWORD'],
            'HOST': creds['RDS_HOST'],
            'PORT': creds['RDS_PORT'],
            'DATABASE': creds['RDS_DATABASE']
        })
        with engine.connect() as connection:
            result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public';"))
            tables = [row[0] for row in result]
        return tables

    def upload_to_db(self, data_frame, table_name):
        """
        Creates a method to upload a Pandas DataFrame to a specified table in the database.
        This allows us to store cleaned data in our local sales_data database.

        # creds = self.read_db_creds('db_creds_local.yaml'): Reads the credentials for the local writable database.
        # engine = self.init_db_engine(creds): Initializes the database engine with local credentials.
        # data_frame.to_sql(table_name, engine, if_exists='replace', index=False): Uploads the DataFrame to the specified table in the database, replacing the table if it exists.
        """
        creds = self.read_db_creds('db_creds_local.yaml')
        engine = self.init_db_engine({
            'USER': creds['LOCAL_USER'],
            'PASSWORD': creds['LOCAL_PASSWORD'],
            'HOST': creds['LOCAL_HOST'],
            'PORT': creds['LOCAL_PORT'],
            'DATABASE': creds['LOCAL_DATABASE']
        })
        data_frame.to_sql(table_name, engine, if_exists='replace', index=False)

if __name__ == '__main__':
    pass
