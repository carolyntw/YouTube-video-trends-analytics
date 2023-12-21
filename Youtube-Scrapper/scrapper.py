import requests, sys, time, re, s3fs

# list of properties to collect under snippet objects
snippet_features = ["title", "publishedAt", "channelId", "channelTitle", "categoryId"]

# Characters that may become problematic in CSV files
unsafe_characters = ['\n', '"']

# Used to identify columns, currently hardcoded order
header = ["video_id"] + snippet_features + ["duration", "subscriber_count", "trending_date", "tags", "view_count", "likes",
                                            "comment_count", "thumbnail_link", "comments_disabled",
                                            "ratings_disabled", "description"]

def prepare_feature(feature):
    """Remove unsafe characters from given data featur."""
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'

def parse_duration(duration):
    """Convert ISO 8601 duration format to total seconds."""
    # Regular expression to extract hours, minutes, and seconds
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration)
    hours, minutes, seconds = 0, 0, 0

    if match:
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        seconds = int(match.group(3)) if match.group(3) else 0
    return hours * 3600 + minutes * 60 + seconds

def get_channel_statistics(channel_id, api_key):
    """Retrieve statistics for a specific YouTube channel."""
    channel_url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics&id={channel_id}&key={api_key}"
    response = requests.get(channel_url)
    if response.status_code != 200:
        return "0"
    channel_data = response.json()
    return channel_data['items'][0]['statistics'].get('subscriberCount', '0')

def api_request(page_token, country_code, api_key):
    """Make an HTTP request."""
    request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet,contentDetails&{page_token}chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}"
    request = requests.get(request_url)
    if request.status_code == 429:
        print("Temp-Banned due to excess requests, please wait and continue later")
        sys.exit()
    return request.json()

def get_tags(tags_list):
    """Process a list of tags."""
    return prepare_feature("|".join(tags_list))

def get_videos(items, api_key):
    """Process a list of video items."""
    lines = []
    for video in items:
        comments_disabled = False
        ratings_disabled = False

        if "statistics" not in video:
            continue

        video_id = prepare_feature(video['id'])
        snippet = video['snippet']
        statistics = video['statistics']
        content_details = video['contentDetails']

        features = [prepare_feature(snippet.get(feature, "")) for feature in snippet_features]
        duration = content_details.get('duration', '')
        duration_in_seconds = parse_duration(duration)
        subscriber_count = prepare_feature(get_channel_statistics(snippet['channelId'], api_key))

        description = snippet.get("description", "")
        thumbnail_link = snippet.get("thumbnails", dict()).get("default", dict()).get("url", "")
        trending_date = time.strftime("%y.%d.%m")
        tags = get_tags(snippet.get("tags", ["[none]"]))
        view_count = statistics.get("viewCount", 0)

        if 'likeCount' in statistics:
            likes = statistics['likeCount']
        else:
            ratings_disabled = True
            likes = 0

        if 'commentCount' in statistics:
            comment_count = statistics['commentCount']
        else:
            comments_disabled = True
            comment_count = 0

        line = [video_id] + features + [str(duration_in_seconds), subscriber_count, prepare_feature(trending_date), tags, prepare_feature(view_count), prepare_feature(likes),
                                        prepare_feature(comment_count), prepare_feature(thumbnail_link), prepare_feature(comments_disabled),
                                        prepare_feature(ratings_disabled), prepare_feature(description)]
        lines.append(",".join(line))
    return lines

def get_pages(country_code, api_key, next_page_token="&"):
    """Retrieve and compile data for YouTube videos from a specific country."""
    country_data = []

    while next_page_token is not None:
        video_data_page = api_request(next_page_token, country_code, api_key)

        next_page_token = video_data_page.get("nextPageToken", None)
        next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else next_page_token

        items = video_data_page.get('items', [])
        country_data += get_videos(items, api_key)
    return country_data

def write_to_s3(country_code, country_data, bucket_name):
    """Write the collected data to a CSV file in the specified S3 bucket."""
    fs = s3fs.S3FileSystem(anon=False)
    s3_path = f"{bucket_name}/{time.strftime('%Y-%m-%d')}_{country_code}_videos.csv"
    
    with fs.open(s3_path, 'w') as file:
        for row in country_data:
            file.write(f"{row}\n")
    print(f"Data written to {s3_path} in S3 bucket.")

def run_youtube_etl(api_key, country_codes):
    """Run the YouTube ETL process."""
    bucket_name = "youtube-analysis-airflow"

    with open('api_key.txt', 'r') as file:
        api_key = file.readline().strip()
    with open('country_codes.txt', 'r') as file:
        country_codes = [line.strip() for line in file]
    for country_code in country_codes:
        country_data = [",".join(header)] + get_pages(country_code, api_key)
        write_to_s3(country_code, country_data, bucket_name)
