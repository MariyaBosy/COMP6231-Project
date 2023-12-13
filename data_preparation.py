# data_preparation.py

import pandas as pd
from sklearn.preprocessing import StandardScaler
import os
from io import StringIO  # Import StringIO from io module


def load_data(file_path):
    try:
        dtype_options = {
            'Host Response Rate': str,
            'Price': float,
            # Add more columns as needed
        }

        # Read the CSV file
        df = pd.read_csv(file_path, encoding='latin1', dtype=dtype_options, low_memory=False)
        return df

    except pd.errors.ParserError as e:
        print(f"Error loading data: {e}")

        # Read the CSV file line by line, skipping problematic lines
        lines = []

        with open(file_path, 'r', encoding='latin1') as file:
            for line_number, line in enumerate(file, start=1):
                # Attempt to use multiple delimiters
                for delimiter in [',', ';']:
                    try:
                        if len(line.split(delimiter)) == len(df.columns):
                            lines.append(line)
                    except (NameError, UnboundLocalError):
                        # If df is not defined, initialize it as None
                        df = None
                        print(f"NameError or UnboundLocalError occurred at line {line_number}: {line}")

        if df is not None and len(lines) > 0:
            # Create a DataFrame from the filtered lines using io.StringIO
            try:
                df = pd.read_csv(StringIO('\n'.join(lines)), dtype=dtype_options, low_memory=False)
                return df
            except Exception as e:
                print(f"Error creating DataFrame from filtered lines: {e}")
                return None
        else:
            return None


def clean_data(df):
    if df is not None:
        # Drop duplicates
        df = df.drop_duplicates()

        # Drop rows with missing values
        df = df.dropna()

    return df

def transform_data(df):
    if df is not None:
        # Convert percentage columns to numeric
        df['Host Response Rate'] = pd.to_numeric(df['Host Response Rate'].str.rstrip('%'), errors='coerce')

        # Extract year from 'Calendar last scraped'
        df['Calendar last scraped'] = pd.to_datetime(df['Calendar last scraped'])
        df['Year'] = df['Calendar last scraped'].dt.year

    return df

def feature_engineering(df):
    if df is not None:
        # Convert column names to lowercase
        df.columns = df.columns.str.lower()

        # One-hot encode 'room type' column
        df = pd.get_dummies(df, columns=['room type'])

    return df

def scale_data(df):
    if df is not None:
        # Scale numerical features
        scaler = StandardScaler()
        df[['latitude', 'longitude']] = scaler.fit_transform(df[['latitude', 'longitude']])

    return df

def save_data(df, output_file):
    # Save the cleaned and prepared data to a new CSV file
    df.to_csv(output_file, index=False)

def process_data_files(file_paths):
    for file_path in file_paths:
        try:
            # Load the dataset
            df = load_data(file_path)

            # Data preparation steps
            df = clean_data(df)
            df = transform_data(df)
            df = feature_engineering(df)
            df = scale_data(df)

            # Save the prepared data to a new file
            output_file = os.path.splitext(file_path)[0] + "_prepared.csv"
            save_data(df, output_file)

            print(f"Data preprocessing completed for {file_path}. Prepared data saved to {output_file}")
        except Exception as e:
            print(f"Error during data preprocessing for {file_path}: {e}")

if __name__ == "__main__":
    file_paths = ["./files/airbnb_ratings_new.csv", "./files/airbnb_sample.csv",  "./files/LA_Listings.csv", "./files/NY_Listings.csv","./files/airbnb-reviews.csv"]
    process_data_files(file_paths)
