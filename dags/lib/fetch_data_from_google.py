from pytrends.request import TrendReq
import os
import csv
from datetime import date, timedelta

def fetch_data_from_google(keyword):
    trends = visualize_related_topics_from_google(keyword)
    store_google_data(trends, keyword)

def visualize_related_topics_from_google(keyword):
    # Initialize pytrends
    pytrend = TrendReq()

    # Calculate the start and end dates
    end_date = date.today()
    start_date_top = end_date - timedelta(days=90)
    start_date_rising = end_date - timedelta(days=30)

    # Build the search query for top research
    pytrend.build_payload(kw_list=[keyword], timeframe=f"{start_date_top} {end_date}")

    # Get top research topics
    top_topics = pytrend.related_topics()

    # Build the search query for rising topics
    pytrend.build_payload(kw_list=[keyword], timeframe=f"{start_date_rising} {end_date}")

    # Get rising topics
    rising_topics = pytrend.related_topics()

    return top_topics, rising_topics

def store_google_data(trends, keyword):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = "/home/choco/airflow/datalake/raw/Google/Datacsv/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)
    print("Writing here: ", TARGET_PATH)

    # Extract the data from the dictionaries
    top_topics = trends[0][keyword]['top'].iloc[1:]
    rising_topics = trends[1][keyword]['rising'].iloc[1:]

    # Select the desired columns
    top_topics = top_topics[['value', 'formattedValue', 'link', 'topic_mid', 'topic_title']]
    rising_topics = rising_topics[['value', 'formattedValue', 'link', 'topic_mid', 'topic_title']]

    # Save the DataFrame to CSV files
    file_path_top = TARGET_PATH + "google-data-top.csv"
    top_topics.head(50).to_csv(file_path_top, index=False)

    file_path_rising = TARGET_PATH + "google-data-rising.csv"
    rising_topics.head(50).to_csv(file_path_rising, index=False)

# Call the function
keyword = 'cosmetics'
fetch_data_from_google(keyword)