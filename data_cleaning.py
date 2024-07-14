import pandas as pd
import re


class DataCleaning:
    def clean_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans user data DataFrame by handling missing values and formatting issues.

        :param df: DataFrame containing user data.
        :return: Cleaned DataFrame.
        """
        df = df.dropna(subset=['date_of_birth', 'join_date'])
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        df['country_code'] = df['country_code'].str.upper()
        df['index'] = pd.to_numeric(df['index'], errors='coerce').astype('Int64')
        df['first_name'] = df['first_name'].str.title()
        df['last_name'] = df['last_name'].str.title()
        df['company'] = df['company'].str.strip().str.title()
        df['address'] = df['address'].str.strip().str.title()
        df['user_uuid'] = df['user_uuid'].str.strip()
        df['email_address'] = df['email_address'].str.strip().str.lower()
        df = df[df['email_address'].apply(lambda x: re.match(r'^\S+@\S+\.\S+$', x) is not None)]
        return df

    def clean_card_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the card data DataFrame by removing erroneous values, NULL values, and formatting errors.

        :param df: DataFrame containing card data.
        :return: Cleaned DataFrame.
        """
        df["card_number"] = df['card_number'].apply(lambda x: str(x).isdigit() if pd.notna(x) else False)
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        df.dropna(subset=['expiry_date'], inplace=True)
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df.dropna(subset=['date_payment_confirmed'], inplace=True)
        
        # Ensure that 'card_number' remains boolean and other columns are cleaned appropriately
        for column in df.columns:
            if column not in ['card_number', 'card_provider']:
                if df[column].dtype == 'object':
                    df[column] = pd.to_numeric(df[column], errors='coerce')
        
        return df

    def clean_store_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the store data DataFrame by removing erroneous values, NULL values, and formatting errors.

        :param df: DataFrame containing store data.
        :return: Cleaned DataFrame.
        """
        df = df.drop(columns=['lat'])
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce').astype('Int64')
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
        df = df.dropna(subset=['staff_numbers', 'opening_date'])
        df['continent'] = df['continent'].replace('eeEurope', 'Europe')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['address'] = df['address'].str.title()
        df['country_code'] = df['country_code'].str.upper()
        return df

    def clean_product_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert product weights to kilograms.

        This method processes the 'weight' column in the provided DataFrame to ensure all weights
        are represented in kilograms. It handles weights given in various units such as kg, g, and ml.

        :param df: DataFrame containing product data with a 'weight' column.
        :return: DataFrame with weights converted to kilograms as float.
        """
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
        df = df.dropna(subset=['EAN', 'date_added'])

        def convert_weight(weight):
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
                        pass
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

        df['weight'] = df['weight'].apply(convert_weight)
        return df

    def clean_orders_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the orders data DataFrame by removing unnecessary columns.

        :param df: DataFrame containing orders data.
        :return: Cleaned DataFrame.
        """
        df.drop(columns=['1', 'first_name', 'last_name', 'level_0'], inplace=True)
        return df

    def clean_date_events_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the date events data DataFrame by ensuring proper formatting and handling erroneous values.

        :param df: DataFrame containing date events data.
        :return: Cleaned DataFrame.
        """
        df['month'] = pd.to_numeric(df['month'], errors='coerce')
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['day'] = pd.to_numeric(df['day'], errors='coerce')
        df = df.dropna(subset=['timestamp', 'month', 'year', 'day'])

        def combine_datetime(row):
            try:
                return pd.to_datetime(f"{int(row['year'])}-{int(row['month']):02d}-{int(row['day']):02d} {row['timestamp']}", format='%Y-%m-%d %H:%M:%S', errors='coerce')
            except ValueError:
                return pd.NaT

        df['timestamp'] = df.apply(combine_datetime, axis=1)
        df = df.drop(columns=['day', 'month', 'year', 'time_period'])
        return df


    def clean_date_events_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the date events data DataFrame by ensuring proper formatting and handling erroneous values.

        :param df: DataFrame containing date events data.
        :return: Cleaned DataFrame.
        """
        df['month'] = pd.to_numeric(df['month'], errors='coerce')
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        df['day'] = pd.to_numeric(df['day'], errors='coerce')
        df = df.dropna(subset=['timestamp', 'month', 'year', 'day'])

        def combine_datetime(row):
            try:
                return pd.to_datetime(f"{int(row['year'])}-{int(row['month']):02d}-{int(row['day']):02d} {row['timestamp']}", format='%Y-%m-%d %H:%M:%S', errors='coerce')
            except ValueError:
                return pd.NaT

        df['timestamp'] = df.apply(combine_datetime, axis=1)
        df = df.drop(columns=['day', 'month', 'year', 'time_period'])
        return df


if __name__ == '__main__':
    pass
