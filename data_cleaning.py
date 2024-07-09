import pandas as pd
import re


class DataCleaning:
    def clean_user_data(self, df):
        """
        Cleans user data DataFrame by handling missing values and formatting issues.

        :param df: DataFrame containing user data.
        :return: Cleaned DataFrame.
        """
        # Drop rows with NaN values in critical columns
        df = df.dropna(subset=['date_of_birth', 'join_date'])

        # not going to clean the phone data as I believe the data itself isn't supposed to be uniform.
        # perhaps it would be better to seperate into seperate country specific data by the country codes, then format the specific data in those seperte dfs.

        # Convert 'join_date' to datetime format
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')

        # Convert 'date_of_birth' to datetime format
        df['date_of_birth'] = pd.to_datetime(
            df['date_of_birth'], errors='coerce')

        # Convert 'country_code' to uppercase
        df['country_code'] = df['country_code'].str.upper()

        # Convert 'index' to numeric and ensure integer type
        try:
            df['index'] = pd.to_numeric(df['index']).astype('Int64')
        except ValueError:
            print("ValueError encountered while converting 'index' to numeric.")

        # Convert 'first_name' and 'last_name' to title case
        df['first_name'] = df['first_name'].str.title()
        df['last_name'] = df['last_name'].str.title()

        # Strip whitespace and convert 'company' and 'address' to title case
        df['company'] = df['company'].str.strip().str.title()
        df['address'] = df['address'].str.strip().str.title()

        # Strip whitespace from 'user_uuid'
        df['user_uuid'] = df['user_uuid'].str.strip()

        # Clean 'email_address' by stripping whitespace and converting to lowercase
        df['email_address'] = df['email_address'].str.strip().str.lower()
        # Keep only rows with valid email addresses
        df = df[df['email_address'].apply(
            lambda x: re.match(r'^\S+@\S+\.\S+$', x) is not None)]

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
        df['staff_numbers'] = pd.to_numeric(
            df['staff_numbers'], errors='coerce')

        # Convert 'staff_numbers' to integer
        df['staff_numbers'] = df['staff_numbers'].astype('Int64')

        # Convert 'opening_date' to datetime
        df['opening_date'] = pd.to_datetime(
            df['opening_date'], errors='coerce')  # Convert to datetime

        # Drop rows with NaN values in 'staff_numbers' and 'opening_date' columns
        df = df.dropna(subset=['staff_numbers', 'opening_date'])

        # Replace 'eeEurope' with 'Europe' in the 'continent' column
        df['continent'] = df['continent'].replace('eeEurope', 'Europe')

        # Convert 'longitude' and 'latitude' to float
        try:
            df['longitude'] = pd.to_numeric(
                df['longitude'])  # Convert to float
        except ValueError as e:
            print(e)
            print("ValueError encountered while converting 'longitude' to numeric.")

        try:
            df['latitude'] = pd.to_numeric(df['latitude'])  # Convert to float
        except ValueError as e:
            print(e)
            print("ValueError encountered while converting 'latitude' to numeric.")

        # Standardize text format
        # Standardize address text format
        df['address'] = df['address'].str.title()
        # Convert country codes to upper case
        df['country_code'] = df['country_code'].str.upper()

        # Save the DataFrame to a CSV file
        # df.to_csv('cleaned_stores_data_sample.csv', index=False)
        # open with DataPreview extension

        return df  # Make sure to return the cleaned DataFrame

    def clean_product_data(self, df):
        """
        Convert product weights to kilograms.

        This method processes the 'weight' column in the provided DataFrame to ensure all weights
        are represented in kilograms. It handles weights given in various units such as kg, g, and ml.

        :param df: DataFrame containing product data with a 'weight' column.
        :return: DataFrame with weights converted to kilograms as float.
        """
        # Convert 'date_added' to datetime format
        df.loc[:, 'date_added'] = pd.to_datetime(
            df['date_added'], errors='coerce')

        # Drop rows with NaN values in 'staff_numbers' and 'opening_date' columns
        df = df.dropna(subset=['EAN', 'date_added'])

        def convert_weight(weight):
            """
            Convert individual weight values to kilograms.

            :param weight: Weight value as a string.
            :return: Weight in kilograms as a float.
            """
            # Ensure the weight is a string and normalize it
            weight = str(weight).lower().strip()

            # If weight is in the format like '12 x 100g'
            if 'x' in weight:
                parts = weight.split('x')
                if len(parts) == 2:
                    try:
                        quantity = int(parts[0].strip())  # Extract quantity
                        # Extract unit weight
                        unit_weight = re.sub(r'[^\d.]', '', parts[1].strip())
                        if 'kg' in parts[1]:
                            return quantity * float(unit_weight)
                        elif 'g' in parts[1]:
                            return quantity * float(unit_weight) / 1000
                        elif 'ml' in parts[1]:
                            return quantity * float(unit_weight) / 1000
                    except ValueError:
                        pass  # If conversion fails, return 0
                return 0

            # If weight is in kilograms
            if 'kg' in weight:
                return float(weight.replace('kg', '').strip().rstrip('.'))
            # If weight is in grams
            elif 'g' in weight:
                return float(weight.replace('g', '').strip().rstrip('.')) / 1000
            # If weight is in milliliters (assuming 1ml = 1g for approximation)
            elif 'ml' in weight:
                return float(weight.replace('ml', '').strip().rstrip('.')) / 1000
            else:
                # Remove non-numeric characters and convert to float
                clean_weight = re.sub(r'[^\d.]+', '', weight).rstrip('.')
                return float(clean_weight) / 1000 if clean_weight else 0

        # Apply the conversion to each value in the 'weight' column
        df['weight'] = df['weight'].apply(convert_weight)
        return df  # Return the updated DataFrame

    def clean_orders_data(self, df):
        """
        Cleans the orders data DataFrame by removing unnecessary columns.

        :param df: DataFrame containing orders data.
        :return: Cleaned DataFrame.
        """
        # Drop the columns 'first_name', 'last_name', and '1'
        df = df.drop(columns=['1'])

        # Additional cleaning steps can be added here if necessary

        return df


if __name__ == '__main__':
    pass
