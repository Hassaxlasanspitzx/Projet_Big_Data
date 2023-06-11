import pandas as pd

# Specify the path to your Parquet file
parquet_file1 ="/home/choco/airflow/datalake/formatted/Google/20230611/google-data-top.parquet"
parquet_file2 ="/home/choco/airflow/datalake/formatted/Youtube/20230611/youtube-data-top.parquet"

# Read the Parquet file into a pandas DataFrame
df1 = pd.read_parquet(parquet_file1)

df2 = pd.read_parquet(parquet_file2)

# Now you can work with the DataFrame
print(df1.head())
print(df2.head())