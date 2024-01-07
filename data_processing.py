import boto3, pandas as pd, csv
from io import StringIO

# Retrieve the list of existing buckets
s3 = boto3.client('s3')

# List of DataFrames, one for each CSV file
dfs = []

# Specify your bucket name
bucket_name = 'youtube-analysis-airflow'

# Get the list of all keys (file names) in the bucket
response = s3.list_objects_v2(Bucket=bucket_name)

def count_tags(tags_value):
    """Count tags amount"""
    if pd.isna(tags_value) or tags_value == '[none]':
        return 0
    else: 
        tags = [tag.strip().lower() for tag in tags_value.split('|')]
        unique_tags = set(tags)
        return len(unique_tags)
    
def viral_score(df):
    """Calculate viral score"""
    # Calculate the minimum and maximum for each column
    min_values = df[['view_count', 'engagement_rate', 'average_daily_views']].min()
    max_values = df[['view_count', 'engagement_rate', 'average_daily_views']].max()
    # Apply the Min-Max scaling formula
    df['view_count_normalized'] = (df['view_count'] - min_values['view_count']) / (max_values['view_count'] - min_values['view_count'])
    df['engagement_rate_normalized'] = (df['engagement_rate'] - min_values['engagement_rate']) / (max_values['engagement_rate'] - min_values['engagement_rate'])
    df['average_daily_views_normalized'] = (df['average_daily_views'] - min_values['average_daily_views']) / (max_values['average_daily_views'] - min_values['average_daily_views'])
    df['viral_score'] = (0.5 * df['view_count_normalized'] + 0.2 * df['engagement_rate_normalized'] + 0.3 * df['average_daily_views_normalized']).round(2)
    return df

def preprocess_dataframe(df,key):
    """Format date columns"""
    # convert data type
    df['publishedAt'] = pd.to_datetime(df['publishedAt'])
    df['trending_date'] = pd.to_datetime(df['trending_date'], format='%y.%d.%m')
    df[['duration', 'subscriber_count', 'view_count', 'comment_count']] = df[['duration', 'subscriber_count', 'view_count', 'comment_count']].astype(int)
    # Convert categorical data into descriptive labels
    category_mapping = {
        1: "Film & Animation",
        2: "Autos & Vehicles",
        10: "Music",
        15: "Pets & Animals",
        17: "Sports",
        18: "Short Movies",
        19: "Travel & Events",
        20: "Gaming",
        21: "Videoblogging",
        22: "People & Blogs",
        23: "Comedy",
        24: "Entertainment",
        25: "News & Politics",
        26: "Howto & Style",
        27: "Education",
        28: "Science & Technology",
        29: "Nonprofits & Activism",
        30: "Movies",
        31: "Anime/Animation",
        32: "Action/Adventure",
        33: "Classics",
        34: "Comedy",
        35: "Documentary",
        36: "Drama",
        37: "Family",
        38: "Foreign",
        39: "Horror",
        40: "Sci-Fi/Fantasy",
        41: "Thriller",
        42: "Shorts",
        43: "Shows",
        44: "Trailers"
    }

    df['categoryId'] = df['categoryId'].map(category_mapping)
    # add total tags value
    df['tags_count'] = df['tags'].apply(count_tags)
    # add an engagement rate column
    df['engagement_rate'] = ((df['likes'] + df['comment_count']) / df['view_count']).round(2)
    # Categorizing videos into segments based on duration
    df['duration_category'] = pd.cut(df['duration'], 
                                   bins=[0, 300, 1200, float('inf')], 
                                   labels=['Short (<5 min)', 'Medium (5-20 min)', 'Long (>20 min)'])
    # convert duration in minutes
    df['duration'] = (df['duration']/60).round().astype(int)
    # add time to trend column
    df['time_to_trend'] = (df['trending_date'].dt.tz_localize(None) - df['publishedAt'].dt.tz_localize(None)).dt.days
    # Extract the hour part of the datetime
    df['hourPublished'] = df['publishedAt'].dt.hour 
    # add a country column
    country_code = key.split('_')[1]
    df['country'] = country_code
    # add a average daily views
    df['average_daily_views'] = df.apply(lambda x: (x['view_count'] / 1) if x['time_to_trend'] == 0 else (x['view_count'] / x['time_to_trend']), axis=1).round().astype(int)
    return df

def concatenate_dataframes(dfs):
    """Concatenate all countries dataframe"""
    return pd.concat(dfs, ignore_index=True)

def process_data():
    """main"""
    s3 = boto3.client('s3')
    bucket_name = 'youtube-analysis-airflow'
    response = s3.list_objects_v2(Bucket=bucket_name)
    dfs = []
    for item in response['Contents']:
        key = item['Key']
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        df = preprocess_dataframe(df, key)
        df = viral_score(df)
        dfs.append(df)
    combined_df = concatenate_dataframes(dfs)
    output_filename = "./data/combined_data.csv"
    combined_df.to_csv(output_filename, index=False, quoting=csv.QUOTE_ALL)
    s3_key = 'combined_data.csv'
    s3.upload_file(Filename=output_filename, Bucket=bucket_name, Key=s3_key)
