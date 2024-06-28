import pandas as pd

class DataCleaning:
    def clean_user_data(self, df):
        print("DataFrame columns:", df.columns)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        else:
            print("Warning: 'date' column not found in DataFrame")
        if 'value' in df.columns:
            df = df[df['value'].apply(lambda x: isinstance(x, (int, float)))]
        else:
            print("Warning: 'value' column not found in DataFrame")
        df.dropna(inplace=True)
        return df

    def clean_card_data(self, df):
        """
        Cleans the card data DataFrame by removing erroneous values,
        NULL values, and formatting errors.
        """
        # Drop rows with any NULL values
        df.dropna(inplace=True)
        
        # Convert card_number to string and remove any rows with invalid card numbers or expiry dates
        df['card_number'] = df['card_number'].astype(str)
        df = df[df['card_number'].apply(lambda x: x.isdigit())]
        
        # Ensure the expiry_date is in the correct format
        df.loc[:, 'expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%y', errors='coerce')
        df.dropna(subset=['expiry_date'], inplace=True)

        # Convert the date_payment_confirmed to datetime format
        df.loc[:, 'date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df.dropna(subset=['date_payment_confirmed'], inplace=True)

        return df

if __name__ == '__main__':
    pass
