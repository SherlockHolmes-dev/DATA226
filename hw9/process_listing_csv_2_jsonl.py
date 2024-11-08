import pandas as pd
import json

def combine_features(row):
    """Combines the description text for better searchability."""
    try:
        return row['description']
    except:
        print("Error:", row)
        return ""

def process_listings_csv(input_file, output_file):
    """
    Processes a listings CSV file to create a JSON format compatible with search indexing.

    This function reads a CSV file containing listings data, processes the data to
    generate new columns for text search, and outputs a JSON file with the necessary
    fields (`put` and `fields`) for indexing documents in a Vespa-compatible format.

    Args:
        input_file (str): The path to the input CSV file containing the listings data.
        output_file (str): The path to the output JSON file to save the processed data.

    Workflow:
        1. Reads the CSV file into a Pandas DataFrame.
        2. Fills missing values in 'listing_url' and 'description' columns with empty strings.
        3. Creates a "text" column that combines specified features using the `combine_features` function.
        4. Selects and renames columns to match required format: 'doc_id', 'title', and 'text'.
        5. Constructs a JSON-like 'fields' column that includes the record's data.
        6. Creates a 'put' column based on 'doc_id' to uniquely identify each document.
        7. Outputs the processed data to a JSON file in a Vespa-compatible format.

    Returns:
        None. Writes the processed DataFrame to `output_file` as a JSON file.
    """
    # Load CSV file
    listings = pd.read_csv(input_file)

    # Fill missing values in required columns
    listings['listing_url'] = listings['listing_url'].fillna('')
    listings['description'] = listings['description'].fillna('')

    # Create a new column 'text' combining the description
    listings["text"] = listings.apply(combine_features, axis=1)

    # Select and rename columns
    listings = listings[['id', 'listing_url', 'text']]
    listings.rename(columns={'listing_url': 'title', 'id': 'doc_id'}, inplace=True)

    # Create 'fields' column as JSON-like structure of each record
    listings['fields'] = listings.apply(lambda row: row.to_dict(), axis=1)

    # Create 'put' column based on 'doc_id'
    listings['put'] = listings['doc_id'].apply(lambda x: f"id:hybrid-search:doc::{x}")

    # Select only 'put' and 'fields' for the output format
    df_result = listings[['put', 'fields']]

    # Write to JSONL format
    df_result.to_json(output_file, orient='records', lines=True)

# Example usage:
process_listings_csv("listings.csv", "clean_listings.jsonl")
