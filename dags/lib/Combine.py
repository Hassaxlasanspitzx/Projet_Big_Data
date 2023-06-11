import pandas as pd
import os

# Specify the directory paths for the formatted files
google_directory = '/home/choco/airflow/datalake/formatted/Google/20230611'
youtube_directory = '/home/choco/airflow/datalake/formatted/Youtube/20230611'

# Get the list of files in the Google directory
google_files = os.listdir(google_directory)

# Initialize an empty DataFrame to store the combined data
combined_data = pd.DataFrame()

# Iterate over the Google files
for file in google_files:
    # Read each file into a DataFrame
    file_path = os.path.join(google_directory, file)
    google_data = pd.read_parquet(file_path)

    # concat the data to the combined DataFrame
    combined_data = pd.concat([combined_data, google_data], ignore_index=True)

# Get the list of files in the YouTube directory
youtube_files = os.listdir(youtube_directory)

# Iterate over the YouTube files
for file in youtube_files:
    # Read each file into a DataFrame
    file_path = os.path.join(youtube_directory, file)
    youtube_data = pd.read_parquet(file_path)

    # concat the data to the combined DataFrame
    combined_data = pd.concat([combined_data, youtube_data], ignore_index=True)

# Perform any necessary analysis or calculations on the combined data
# For example, you can group by topic and count the occurrences to find the most trending topics

# Group by topic and count occurrences
trending_topics = combined_data.groupby('topic_title').size().reset_index(name='count')

# Sort the topics by count in descending order
trending_topics = trending_topics.sort_values('count', ascending=False)

# Print the most trending topics
print("Most Trending Topics:")
print(trending_topics.head())

# Specify the output directory path
output_directory = '/home/choco/airflow/datalake/usage/analytics'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Save the combined data to a new file
output_file = os.path.join(output_directory, 'most_trending_topics.csv')
combined_data.to_csv(output_file, index=False)
print("Combined data saved to:", output_file)