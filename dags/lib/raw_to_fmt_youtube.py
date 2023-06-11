import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import date

# Specify the file path relative to the current script
file_path = '/home/choco/airflow/datalake/raw/Youtube/Datacsv/20230611/youtube-data-top.csv'

# Read the CSV file
youtube_data = pd.read_csv(file_path)

# Perform transformations and normalizations
youtube_data['Title'] = youtube_data['Title'].str.lower()  # Convert to lowercase
youtube_data['Title'] = youtube_data['Title'].str.strip()  # Remove leading/trailing whitespace
# Apply other transformations and normalizations as needed

# Specify the output directory path
current_day = date.today().strftime("%Y%m%d")
output_directory = '/home/choco/airflow/datalake/formatted/Youtube/' + current_day + "/"
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Specify the output file path within the directory
output_file_youtube = os.path.join(output_directory, 'youtube-data-top.parquet')

# Convert the DataFrame to Parquet format and write to file
table = pa.Table.from_pandas(youtube_data)
pq.write_table(table, output_file_youtube)

print("YouTube data formatted and saved to:", output_file_youtube)