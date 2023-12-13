# data_preparation.py

import pandas as pd
from sklearn.preprocessing import StandardScaler

def load_data(file_path):
    try:
        # Explicitly specify data types for columns with mixed types
        dtype_options = {
            'Host Response Rate': str,  # or int or float, depending on your data
            'Price': float,  # or int, depending on your data
            # Add more columns as needed
        }

        # Disable low_memory mode
        df = pd.read_csv(file_path, encoding='latin1', dtype=dtype_options, low_memory=False)

        # Print data types of all columns
        # print(df.dtypes)
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def clean_data(df):
    # Drop duplicates
    df = df.drop_duplicates()

    # Drop rows with missing values
    df = df.dropna()

    return df

def transform_data(df):
    # Convert percentage columns to numeric
    df['Host Response Rate'] = pd.to_numeric(df['Host Response Rate'].str.rstrip('%'), errors='coerce')

    # Extract year from 'Calendar last scraped'
    df['Calendar last scraped'] = pd.to_datetime(df['Calendar last scraped'])
    df['Year'] = df['Calendar last scraped'].dt.year

    return df

def feature_engineering(df):
    # One-hot encode 'Room type' column
    df = pd.get_dummies(df, columns=['Room type'])

    return df

def scale_data(df):
    # Scale numerical features
    scaler = StandardScaler()
    df[['latitude', 'longitude']] = scaler.fit_transform(df[['latitude', 'longitude']])

    return df

def save_data(df, output_file):
    # Save the cleaned and prepared data to a new CSV file
    df.to_csv(output_file, index=False)
