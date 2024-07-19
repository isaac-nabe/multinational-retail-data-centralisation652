import pandas as pd
import re
import os


class DataCleaning:
    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans user data DataFrame by handling missing values and formatting issues.

        :param df: DataFrame containing user data.
        :return: Cleaned DataFrame.
        """
        # Drop rows where all elements are NaN
        df = df.dropna(how='all')

        # Convert date columns to datetime format
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        df['date_of_birth'] = pd.to_datetime(
            df['date_of_birth'], errors='coerce')

        # Standardize string formats
        df['country_code'] = df['country_code'].str.upper()
        df['index'] = pd.to_numeric(
            df['index'], errors='coerce').astype('Int64')
        df['first_name'] = df['first_name'].str.title()
        df['last_name'] = df['last_name'].str.title()
        df['company'] = df['company'].str.strip().str.title()
        df['address'] = df['address'].str.strip().str.title()
        df['user_uuid'] = df['user_uuid'].str.strip()
        df['email_address'] = df['email_address'].str.strip().str.lower()

        # Validate email addresses
        df = df[df['email_address'].apply(
            lambda x: re.match(r'^\S+@\S+\.\S+$', x) is not None)]

        return df

    def clean_card_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the card data DataFrame by removing erroneous values, NULL values, and formatting errors.

        :param df: DataFrame containing card data.
        :return: Cleaned DataFrame.
        """
        # Save the original data to a CSV file for reference
        df.to_csv("og_card_data.csv", index=False)

        # Ensure 'card_number' contains only digit strings and non-null values
        df['card_number'] = df['card_number'].astype(
            str).str.strip().str.replace('?', '', regex=False)
        # consider saving another csv after each operation to check how they impact the data. 
        # df.to_csv("_.csv", index=False)

        # Convert date columns to datetime format
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')

        # Convert non-critical object columns to numeric, where applicable
        for column in df.columns:
            if column not in ['card_number', 'card_provider'] and df[column].dtype == 'object':
                df[column] = pd.to_numeric(df[column], errors='coerce')

        # Filter valid card providers
        valid_card_providers = [
            "Discover", "VISA 13 digit", "VISA 16 digit", "VISA 19 digit",
            "American Express", "Mastercard", "Maestro", "Diners Club / Carte Blanche",
            "JCB 15 digit", "JCB 16 digit"
        ]
        df = df[df['card_provider'].isin(valid_card_providers)]

        # Drop rows where all values are missing
        df.dropna(how='all', inplace=True)

        return df

    def clean_store_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the store data DataFrame by removing erroneous values, NULL values, and formatting errors.

        :param df: DataFrame containing store data.
        :return: Cleaned DataFrame.
        """
        # Convert all column headers to lowercase
        df.columns = df.columns.str.lower()

        # Drop 'lat' column if it exists
        if 'lat' in df.columns:
            df = df.drop(columns=['lat'])

        # Drop rows where all elements are NaN
        df = df.dropna(how='all')

        # Clean 'staff_numbers' by stripping non-numeric characters
        df['staff_numbers'] = df['staff_numbers'].apply(
            lambda x: re.sub(r'\D', '', str(x)) if pd.notna(x) else None)

        # Drop rows with NaN in 'opening_date' column
        df = df.dropna(subset=['opening_date'])

        # Convert columns to appropriate formats
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')

        # Replace erroneous values in 'continent'
        df['continent'] = df['continent'].replace({'eeEurope': 'Europe', 'eeAmerica': 'America'})

        # Clean 'address' and 'country_code'
        df['address'] = df['address'].str.title()
        df['country_code'] = df['country_code'].str.upper()
        df = df[df['country_code'].str.len() <= 2]

        # Drop rows where store_code is the string "NULL"
        df = df[df['store_code'].str.upper() != 'NULL']

        return df

    def clean_product_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert product weights to kilograms.

        This method processes the 'weight' column in the provided DataFrame to ensure all weights
        are represented in kilograms. It handles weights given in various units such as kg, g, and ml.

        # I had to return to this section to make several fixes to the cleaning operations -
        # - as I was returning errors when running my SQL scripts.
        # This is why i defined a 'save_and_log' helper function to just save after every operation to identify problem lines in my code.

        :param df: DataFrame containing product data with a 'weight' column.
        :return: DataFrame with weights converted to kilograms as float.
        """
        def save_and_log(df, filename, step_description):
            df.to_csv(filename, index=False)
            if os.path.exists(filename):
                print(f"{step_description} saved successfully: {filename}")
            else:
                print(f"Failed to save {step_description}: {filename}")

        save_and_log(df, "og_product_data.csv", "Original product data")

        # Drop rows where all elements are NaN
        df = df.dropna(how='all')
        save_and_log(df, "after_dropping_all_na.csv", "After dropping all NaN rows")

        # Convert 'date_added' to datetime
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
        save_and_log(df, "after_converting_date_added.csv", "After converting date_added to datetime")

        # Drop rows where 'date_added' is NaN but 'EAN' is non-numeric
        df = df[~(df['date_added'].isna() & ~
                  df['EAN'].apply(lambda x: str(x).isdigit()))]
        save_and_log(df, "after_dropping_na_date_added_with_non_numeric_ean.csv", "After dropping NaN in date_added with non-numeric EAN")

        def convert_weight(weight):
            """Helper function to caluclate weight conversions in product data."""
            weight = str(weight).lower().strip()
            if 'x' in weight:
                parts = weight.split('x')
                if len(parts) == 2:
                    try:
                        quantity = int(parts[0].strip())
                        unit_weight = re.sub(r'[^\d.]', '', parts[1].strip())
                        if 'kg' in parts[1]:
                            return quantity * float(unit_weight)
                        elif 'g' in parts[1]:
                            return quantity * float(unit_weight) / 1000
                        elif 'ml' in parts[1]:
                            return quantity * float(unit_weight) / 1000
                    except ValueError:
                        return 0
                return 0
            if 'kg' in weight:
                return float(weight.replace('kg', '').strip().rstrip('.'))
            elif 'g' in weight:
                return float(weight.replace('g', '').strip().rstrip('.')) / 1000
            elif 'ml' in weight:
                return float(weight.replace('ml', '').strip().rstrip('.')) / 1000
            else:
                clean_weight = re.sub(r'[^\d.]+', '', weight).rstrip('.')
                return float(clean_weight) / 1000 if clean_weight else 0

        # Apply the conversion function and handle non-numeric weights
        df['weight'] = df['weight'].apply(
            lambda x: convert_weight(x) if isinstance(x, str) else 0)
        save_and_log(df, "after_converting_weights.csv", "After converting weights")

        return df

    def clean_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the orders data DataFrame by removing unnecessary columns.

        :param df: DataFrame containing orders data.
        :return: Cleaned DataFrame.
        """
        # Drop rows where all elements are NaN
        df = df.dropna(how='all')

        # Drop unnecessary columns
        df.drop(columns=['1', 'first_name', 'last_name', 'level_0'], inplace=True)

        return df

    def clean_date_events_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the date events data DataFrame by ensuring proper formatting and handling erroneous values.

        :param df: DataFrame containing date events data.
        :return: Cleaned DataFrame.
        """
        # Drop rows where all elements are NaN
        df = df.dropna(how='all')

        # Convert date components to numeric
        df['month'] = pd.to_numeric(df['month'], errors='coerce')
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['day'] = pd.to_numeric(df['day'], errors='coerce')

        # Drop rows with NaN in critical columns
        df = df.dropna(subset=['timestamp', 'month', 'year', 'day'])

        # Combine date components into a single datetime column
        def combine_datetime(row):
            try:
                return pd.to_datetime(f"{int(row['year'])}-{int(row['month']):02d}-{int(row['day']):02d} {row['timestamp']}", format='%Y-%m-%d %H:%M:%S', errors='coerce')
            except ValueError:
                return pd.NaT

        df['timestamp'] = df.apply(combine_datetime, axis=1)

        return df


if __name__ == '__main__':
    pass
