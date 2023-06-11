import pandas as pd
import os
from datetime import date

# Specify the file path relative to the current script
file_path_top = '/home/choco/airflow/datalake/raw/Google/Datacsv/20230611/google-data-top.csv'

# Read the CSV file
google_data_top = pd.read_csv(file_path_top)

unique_titles_before = google_data_top['topic_title'].unique()
print("Unique titles before transformations:", len(unique_titles_before))

# Perform transformations and normalizations
google_data_top['topic_title'] = google_data_top['topic_title'].str.lower()  # Convert to lowercase
google_data_top['topic_title'] = google_data_top['topic_title'].str.strip()  # Remove leading/trailing whitespace


unique_titles_after = google_data_top['topic_title'].unique()
print("Unique titles after transformations:", len(unique_titles_after))

# Select the desired columns
columns = ['topic_title']
google_data_top_formatted = google_data_top[columns]

print(google_data_top_formatted)

# Specify the output directory path
current_day = date.today().strftime("%Y%m%d")
output_directory = '/home/choco/airflow/datalake/formatted/Google/' + current_day + "/"
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Specify the output file path within the directory
output_file_google_top = os.path.join(output_directory, 'google-data-top.parquet')

# Convert the dataframe to Parquet format
google_data_top_formatted.to_parquet(output_file_google_top, index=False)

print("Google data (top) formatted and saved to:", output_file_google_top)