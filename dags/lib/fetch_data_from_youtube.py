import csv
import os
from googleapiclient.discovery import build
from datetime import date, timedelta

def fetch_data_from_youtube(query):
    # Set up the API client
    api_key = "AIzaSyAtb4oic_3wLmkf7Tm3Lp9PQt26L9Mmg9k"  # Replace with your actual API key
    youtube = build("youtube", "v3", developerKey=api_key)

    # Specify the start and end dates for the desired periods
    end_date = date.today().strftime("%Y-%m-%d")
    start_date_rising = (date.today() - timedelta(days=28)).strftime("%Y-%m-%d")
    start_date_popular = (date.today() - timedelta(days=180)).strftime("%Y-%m-%d")

    # Fetch rising videos
    rising_request = youtube.search().list(
        part="snippet",
        q=query,
        type="video",
        order="viewCount",
        maxResults=50,
        publishedAfter=start_date_rising + "T00:00:00Z",
        publishedBefore=end_date + "T23:59:59Z",
        regionCode="US"
    )
    rising_response = rising_request.execute()

    # Fetch popular video
    popular_request = youtube.search().list(
        part="snippet",
        q=query,
        type="video",
        order="relevance",
        maxResults=50,
        publishedAfter=start_date_popular + "T00:00:00Z",
        publishedBefore=end_date + "T23:59:59Z",
        regionCode="US"
    )
    popular_response = popular_request.execute()

    # Process the API responses
    rising_results = []
    popular_results = []
    for item in rising_response["items"]:
        video_title = item["snippet"]["title"]
        video_url = "https://www.youtube.com/watch?v=" + item["id"]["videoId"]
        rising_results.append({"Title": video_title, "URL": video_url})
    for item in popular_response["items"]:
        video_title = item["snippet"]["title"]
        video_url = "https://www.youtube.com/watch?v=" + item["id"]["videoId"]
        popular_results.append({"Title": video_title, "URL": video_url})

    return rising_results, popular_results

def save_results_to_csv(results, directory_path, file_name):
    # Ensure the directory exists
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

    # Write the results to a CSV file
    file_path = os.path.join(directory_path, file_name)
    fieldnames = ["Title", "URL"]

    with open(file_path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print("Results saved to:", file_path)

# Call the functions
query = "cosmetics"
current_day = date.today().strftime("%Y%m%d")
directory_path = "/home/choco/airflow/datalake/raw/Youtube/Datacsv/" + current_day + "/"

# Fetch and save rising videos
rising_results, _ = fetch_data_from_youtube(query)
rising_file_name = "youtube-data-rising.csv"
save_results_to_csv(rising_results, directory_path, rising_file_name)

# Fetch and save popular video
_, popular_results = fetch_data_from_youtube(query)
popular_file_name = "youtube-data-top.csv"
save_results_to_csv(popular_results, directory_path,popular_file_name)

