import yaml
from sqlalchemy import create_engine, text
import pandas as pd
from typing import Tuple

class DatabaseConnector:
    def read_db_creds(self, file_name: str) -> Tuple[dict, str]:
        """
        Reads the database credentials from a YAML file and returns them as a dictionary
        along with the credentials type ('RDS' or 'LOCAL').

        :param file_name: The name of the YAML file containing the database credentials.
        :return: A tuple containing a dictionary of the database credentials and a string indicating the type.
        """
        with open(file_name, 'r') as file:
            creds = yaml.safe_load(file)
        print(f"Credentials loaded from {file_name}")  # Debugging output

        if 'RDS_USER' in creds:
            creds_type = 'RDS'
        elif 'LOCAL_USER' in creds:
            creds_type = 'LOCAL'
        else:
            raise ValueError("Invalid credentials file. Must contain either RDS_USER or LOCAL_USER.")
        
        return creds, creds_type

    def init_db_engine(self, creds: dict, creds_type: str):
        """
        Initializes and returns a SQLAlchemy database engine using the provided credentials.
        This engine will be used to interact with the database.

        :param creds: A dictionary containing the database credentials.
        :param creds_type: A string specifying the type of credentials ('RDS' or 'LOCAL').
        :return: The initialized SQLAlchemy database engine.
        """
        if creds_type == 'RDS':
            return create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        elif creds_type == 'LOCAL':
            return create_engine(f"postgresql://{creds['LOCAL_USER']}:{creds['LOCAL_PASSWORD']}@{creds['LOCAL_HOST']}:{creds['LOCAL_PORT']}/{creds['LOCAL_DATABASE']}")
        else:
            raise ValueError("Invalid credentials type. Must be 'RDS' or 'LOCAL'.")

    def list_db_tables(self) -> list:
        """
        Lists all tables in the database.

        :return: A list of all table names in the database.
        """
        creds, creds_type = self.read_db_creds('db_creds_rds.yaml')
        engine = self.init_db_engine(creds, creds_type)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public';"))
            return [row[0] for row in result]

    def upload_to_db(self, data_frame: pd.DataFrame, table_name: str):
        """
        Uploads a Pandas DataFrame to a specified table in the database.

        :param data_frame: The Pandas DataFrame to upload.
        :param table_name: The name of the table to upload the data to.
        """
        creds, creds_type = self.read_db_creds('db_creds_local.yaml')
        engine = self.init_db_engine(creds, creds_type)
        data_frame.to_sql(table_name, engine, if_exists='replace', index=False)

if __name__ == '__main__':
    pass
