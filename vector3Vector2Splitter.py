# This script processes a list of CSV files in a directory. For columns containing list-like data, it expands the lists into individual rows.
# Example: A value [1,2,3,4] is transformed into value_x = 1, value_y = 2, value_z = 3, value_w = 4.
# Example: A value [1,2,3] is transformed into value_x = 1, value_y = 2, value_z = 3.
# Example: A value [1,2] is transformed into value_x = 1, value_y = 2.
# The original column is removed from the DataFrame, and only the newly created columns are appended to the DataFrame.


import pandas as pd
import ast
import os

# Set the base paths for input and output directories
base_path = r'<source_directory_path>'
output_path = r'<destination_directory_path>'

# Ensure the output directory exists
os.makedirs(output_path, exist_ok=True)

# Iterate over all files in the input directory
for file_name in os.listdir(base_path):
    if file_name.endswith('.csv'):
        input_file = os.path.join(base_path, file_name)
        output_file = os.path.join(output_path, f"Splitted_{file_name}")

        # Read the CSV file
        df = pd.read_csv(input_file)
        print('processing file '+input_file)

        # Iterate over columns to find list-like values
        columns_to_process = []

        for column in df.columns:
            # Skip columns that have no non-null values
            if df[column].dropna().empty:
                continue

            try:
                # Try converting the first non-null value to a list
                first_value = df[column].dropna().iloc[0]
                parsed_value = ast.literal_eval(first_value)

                # Check if the value is a list
                if isinstance(parsed_value, list):
                    columns_to_process.append(column)
            except (ValueError, SyntaxError):
                # Not a list-like value, continue to the next column
                continue

        # Process the identified columns
        for column in columns_to_process:
            print('Processing column: ' + column)
            # Safely evaluate each string as a list
            split_columns = df[column].apply(ast.literal_eval).apply(pd.Series)

            # Dynamically create new column names based on the original column name
            num_cols = split_columns.shape[1]
            new_column_names = [f'{column}{axis}' for axis in ['_X', '_Y', '_Z', '_W'][:num_cols]]
            print(new_column_names)

            # Set the new column names
            split_columns.columns = new_column_names

            # Drop the original column and add the new columns to the DataFrame
            df = df.drop(columns=[column]).join(split_columns)

        # Save the modified DataFrame to a new CSV file
        df.to_csv(output_file, index=False)

        print(f"Processed CSV saved as {output_file}")
