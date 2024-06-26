import pandas as pd

# define DataCleaning class
class DataCleaning:
    def clean_user_data(self, df):
        # Print DataFrame columns for debugging & helps see the columns present in the DataFrame, useful for debugging.
        print("DataFrame columns:", df.columns)

        # Ensures that it only attempts to clean columns that exist in the DataFrame, preventing KeyError.
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')  # Fix date formats
        else:
            print("Warning: 'date' column not found in DataFrame")

        if 'value' in df.columns:
            df = df[df['value'].apply(lambda x: isinstance(x, (int, float)))]  # Remove incorrectly typed values
        else:
            print("Warning: 'value' column not found in DataFrame")

        df.dropna(inplace=True)  # Remove rows with NULL values

        # Additional cleaning steps as required
        return df


if __name__ == '__main__':
    pass