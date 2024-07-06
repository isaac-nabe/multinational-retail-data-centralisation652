import pandas as pd
import re


class DataCleaning:
    def clean_user_data(self, df):
        """
        Cleans user data DataFrame by handling missing values and formatting issues.

        :param df: DataFrame containing user data.
        :return: Cleaned DataFrame.
        """
        print("DataFrame columns:", df.columns)

        # Remove non-digit characters from 'phone_number'
        df.loc[:, 'phone_number'] = df['phone_number'].str.replace(
            r'\D', '', regex=True)

        # Convert 'join_date' to datetime format
        df.loc[:, 'join_date'] = pd.to_datetime(
            df['join_date'], errors='coerce')

        # Convert 'country_code' to uppercase
        df.loc[:, 'country_code'] = df['country_code'].str.upper()

        # Convert 'index' to numeric and ensure integer type
        try:
            df.loc[:, 'index'] = pd.to_numeric(df['index']).astype('Int64')
        except ValueError:
            print("ValueError encountered while converting 'index' to numeric.")

        # Convert 'first_name' and 'last_name' to title case
        df.loc[:, 'first_name'] = df['first_name'].str.title()
        df.loc[:, 'last_name'] = df['last_name'].str.title()

        # Strip whitespace and convert 'company' and 'address' to title case
        df.loc[:, 'company'] = df['company'].str.strip().str.title()
        df.loc[:, 'address'] = df['address'].str.strip().str.title()

        # Strip whitespace from 'user_uuid'
        df.loc[:, 'user_uuid'] = df['user_uuid'].str.strip()

        # Convert 'date_of_birth' to datetime format
        df.loc[:, 'date_of_birth'] = pd.to_datetime(
            df['date_of_birth'], errors='coerce')

        # Clean 'email_address' by stripping whitespace and converting to lowercase
        df.loc[:, 'email_address'] = df['email_address'].str.strip().str.lower()
        # Keep only rows with valid email addresses
        df = df[df['email_address'].apply(
            lambda x: re.match(r'^\S+@\S+\.\S+$', x) is not None)]

        # Remove rows with any NULL values
        df = df.dropna()

        return df

    def clean_card_data(self, df):
        """
        Cleans the card data DataFrame by removing erroneous values,
        NULL values, and formatting errors.

        :param df: DataFrame containing card data.
        :return: Cleaned DataFrame.
        """
        # Keep rows with valid card numbers
        # NOTE this is where on of your Setting Copy Warnings is coming from you can just
        # use the apply directly on the column. Using this synatx df[df["card_number"]]
        # there is the potential that you might not alter the original dataframe
        # Try this instead
        df["card_number"] = df['card_number'].apply(
            lambda x: str(x).isdigit() if pd.notna(x) else False)
        # df = df[df['card_number'].apply(lambda x: str(x).isdigit() if pd.notna(x) else False)] - this was taking a slice instead of editing the column directly

        # Convert 'expiry_date' to datetime format
        df['expiry_date'] = pd.to_datetime(
            df['expiry_date'], format='%m/%y', errors='coerce')
        # Drop rows with NULL values in 'expiry_date'
        df.dropna(subset=['expiry_date'], inplace=True)

        # Convert 'date_payment_confirmed' to datetime format
        df['date_payment_confirmed'] = pd.to_datetime(
            df['date_payment_confirmed'], errors='coerce')
        # Drop rows with NULL values in 'date_payment_confirmed'
        df.dropna(subset=['date_payment_confirmed'], inplace=True)

        # Convert columns to numeric and handle exceptions explicitly
        for column in df.columns:
            if df[column].dtype == 'object':  # Check if the column is of type 'object'
                try:
                    df[column] = pd.to_numeric(df[column])
                except ValueError:
                    print(f"ValueError encountered while converting '{
                          column}' to numeric. Skipping this column.")
                    pass  # If there's a ValueError, skip the conversion for that column

        return df

    def clean_store_data(self, df):
        """
        Cleans the store data DataFrame by removing erroneous values,
        NULL values, and formatting errors.

        :param df: DataFrame containing store data.
        :return: Cleaned DataFrame.

        1. Drop the 'lat' column.
        2. Replace 'eeEurope' with 'Europe' in the 'continent' column.
        3. Drop rows with NULL values, except when 'store_type' is 'Web Portal'.
        4. Convert 'staff_numbers' to numeric and then to integer.
        5. Convert 'opening_date' to datetime.
        6. Convert 'longitude' and 'latitude' to float.
        7. Standardize text format in 'address' and 'country_code' columns.
        """

        # Drop the 'lat' column
        df = df.drop(columns=['lat'])

        # Convert 'staff_numbers' to numeric, setting errors='coerce' to convert non-numeric values to NaN
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')

        # Convert 'staff_numbers' to integer
        df['staff_numbers'] = df['staff_numbers'].astype('Int64')

        # Convert 'opening_date' to datetime
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')  # Convert to datetime

        # Drop rows with NaN values in 'staff_numbers' and 'opening_date' columns
        df = df.dropna(subset=['staff_numbers', 'opening_date'])

        # Replace 'eeEurope' with 'Europe' in the 'continent' column
        df['continent'] = df['continent'].replace('eeEurope', 'Europe')

        # Convert 'longitude' and 'latitude' to float
        try:
            df['longitude'] = pd.to_numeric(df['longitude'])  # Convert to float
        except ValueError as e:
            print(e)
            print("ValueError encountered while converting 'longitude' to numeric.")

        try:
            df['latitude'] = pd.to_numeric(df['latitude'])  # Convert to float
        except ValueError as e:
            print(e)
            print("ValueError encountered while converting 'latitude' to numeric.")

        # Standardize text format
        df['address'] = df['address'].str.title()  # Standardize address text format
        df['country_code'] = df['country_code'].str.upper()  # Convert country codes to upper case

        # Save the DataFrame to a CSV file
        df.to_csv('cleaned_stores_data_sample.csv', index=False)
        # open with DataPreview extension

        return df  # Make sure to return the cleaned DataFrame




if __name__ == '__main__':
    pass
