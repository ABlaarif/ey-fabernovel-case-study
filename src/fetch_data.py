import requests
import pandas as pd
from google.cloud import storage

def fetch_products_from_api():
    """
    Fetches product data from the Fake Store API.
    This function makes a GET request to the API and retrieves product data in JSON format,
    then converts that JSON into a pandas DataFrame which makes it easier for data manipulation.

    Returns:
        pd.DataFrame: A DataFrame containing product data with columns such as id, title, price, etc.
    """
    api_url = "https://fakestoreapi.com/products"  # URL of the Fake Store API endpoint
    response = requests.get(api_url)  # Send a GET request to the API
    products_data = response.json()  # Parse the JSON response into a Python list of dictionaries
    return pd.DataFrame(products_data)  # Convert list of dictionaries to a DataFrame for easier handling

def save_data_to_csv(dataframe, filename='products_data.csv'):
    """
    Saves the fetched data to a CSV file.
    This function takes a DataFrame and writes it to a CSV file, which is a commonly used format for data storage and transfer.
    The index=False parameter prevents pandas from writing row numbers (indices) into the CSV file.

    Args:
        dataframe (pd.DataFrame): The DataFrame to save to a CSV file.
        filename (str): The name of the file where the data will be saved.
    """
    dataframe.to_csv(filename, index=False)  # Save the DataFrame to a CSV without the index column
    print(f"Data saved to {filename}")

def upload_file_to_google_cloud(bucket_name, source_file_path, destination_file_name, credentials_path):
    """
    Uploads a file to Google Cloud Storage.
    This function initializes a Google Cloud Storage client using a service account JSON key,
    then uploads a file from local storage to the specified bucket in Google Cloud Storage.

    Args:
        bucket_name (str): The name of the bucket where the file will be stored.
        source_file_path (str): The local path of the file to be uploaded.
        destination_file_name (str): The name under which the file will be stored in the bucket.
        credentials_path (str): The path to the service account credentials JSON file.
    """
    storage_client = storage.Client.from_service_account_json(credentials_path)  # Authenticate and create a storage client
    bucket = storage_client.bucket(bucket_name)  # Access the bucket
    blob = bucket.blob(destination_file_name)  # Create a new blob (file) in the bucket
    blob.upload_from_filename(source_file_path)  # Upload the file from the local file system
    print(f"File {source_file_path} uploaded to {destination_file_name} in bucket {bucket_name}")

if __name__ == "__main__":
    products_df = fetch_products_from_api()  # Fetch product data from API and store in DataFrame
    csv_file = 'products_data.csv'  # Define the local CSV file name
    save_data_to_csv(products_df, csv_file)  # Save the DataFrame to a CSV file
    # Use the provided bucket name and JSON path
    bucket_name = 'eyusecase'
    credentials_path = r'C:\Users\abdel\Documents\ey-fabernovel-use-case-a84c18988f30.json'
    upload_file_to_google_cloud(bucket_name, csv_file, csv_file, credentials_path)  # Upload the CSV file to Google Cloud Storage
