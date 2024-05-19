
# If we need to pull data from youtube API
import googleapiclient.discovery
api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyAa0fPsrlLjqflao9ovCUHoPLUP1I6d3XU"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey = api_key)
c_id ="UCtGbExCzlwmsyWKpxLnyEww"

#Pull Channel Data
def channel_data(c_id):
     request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id= c_id
    )
     response = request.execute()

     data = {"channel_id" : response['items'][0]['id'],
             "channel_name" : response['items'][0]['snippet']['title'],
             "channel_view" : response['items'][0]['statistics']['viewCount'],
             "channel_description" : response['items'][0]['snippet']['description'],
             "channel_publishedAt" : response['items'][0]['snippet']['publishedAt'],
             "playlist_id" : response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
             "subscription_count" : response['items'][0]['statistics']['subscriberCount']
             }
     return data
        
#pull Playlist data

def playlist_data(c_id):
     request = youtube.playlists().list(
        part="snippet",
        channelId=c_id
        )
     response = request.execute()

     data ={ "playlist_id":response['items'][0]['id'],
        "channel_id" : response['items'][0]['snippet']['channelId'],
        "playlist_name":response['items'][0]['snippet']['title']
           }

#Pull video data
search_request = youtube.search().list(
    part="id",
    channelId=c_id,
    type="video"
    )
search_response = search_request.execute()

# Extract video IDs from the search results
video_ids = [item['id']['videoId'] for item in search_response['items']]

# Use the video IDs to retrieve detailed information about each video
videos_request = youtube.videos().list(
    part="id,snippet,status,statistics,contentDetails",
    id=','.join(video_ids)
    )
videos_response = videos_request.execute()

def videos_data(c_id):
    video_table = []
    for item in videos_response['items']:
        video_data = {
            "video_id": item['id'],
            "channel_id": item['snippet']['channelId'],
            "video_name": item['snippet']['title'],
            "video_description": item['snippet']['description'],
            "video_publishedat": item['snippet']['publishedAt'],
            "view_count": item['statistics']['viewCount'],
            "like_count": item['statistics']['likeCount'],
            "favoriteCount": item['statistics']['favoriteCount'],
            "commentCount": item['statistics']['commentCount'],
            "duration": item['contentDetails']['duration'],
            "default_thumbnails": item['snippet']['thumbnails']['default']['url'],
            "caption_status": item['contentDetails']['caption']
        }
        video_table.append(video_data)
    return video_table

# Pull Comment data for the respective video
comment_table = []
for video_id in video_ids:
    request = youtube.commentThreads().list(
        part="id,snippet",
        videoId=video_id
    )
    response = request.execute()
    if 'items' in response:
        for item in response['items']:
            comment_table.append({
                'comment_id': item['id'],
                'video_id': item['snippet']['videoId'],
                'comment_text':item['snippet']['topLevelComment']['snippet']['textOriginal'],
                'author_name':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'comment_publishedAt':item['snippet']['topLevelComment']['snippet']['publishedAt']
            })


# To connect SQl with python
import mysql.connector
mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    password ='Root',
    database= 'project'
)  

cursor= mydb.cursor()

#Create a table in SQL with python Query
# Define the SQL query to create the Channel table
create_table_query = '''
    CREATE TABLE IF NOT EXISTS Channel (
        channel_id VARCHAR(255),
        channel_name VARCHAR(255),
        channel_view INT,
        channel_description TEXT,
        playlist_id VARCHAR(255),
        subscription_count INT,
        channel_publishedAt varchar(255)
    )
  '''
# Execute the create table query
cursor.execute(create_table_query)

# Define the SQL query to create the playlist table
playlist_table = '''
    CREATE TABLE IF NOT EXISTS Playlist (
        playlist_id varchar(255),
        channel_id varchar(255),
        playlist_name varchar(255)
        )
        '''
# Execute the create table query
cursor.execute(playlist_table)

# Define the SQL query to create the video table
video_table_query = '''
    CREATE TABLE IF NOT EXISTS Video (
        video_id varchar(255),
        channel_id varchar(255),
        vedio_name varchar(255),
        video_description text,
        video_publishedat varchar(255),
        view_count int,
        like_count int,
        favoriteCount int,
        commentCount int,
        duration varchar(255),
        default_thumbnails varchar(255),
        caption_status varchar(255)
        )
    '''
# Execute the create table query
cursor.execute(video_table_query)

# Define the SQL query to create the comment table
comment_table_query = '''
    CREATE TABLE IF NOT EXISTS comment (
        comment_id varchar(255),
        video_id varchar(255),
        comment_text text,
        author_name varchar(255),
        comment_publishedAt varchar(255)
        )
     '''
# Execute the create table query
cursor.execute(comment_table_query)


# Insert Data in table
# Define the insert query for channel table
insert_query = "INSERT INTO Channel VALUES (%s, %s, %s, %s, %s, %s, %s)"

# Get channel data
channel_data = channel_data(c_id)

data = (
    channel_data['channel_id'],
    channel_data['channel_name'],
    channel_data['channel_view'],
    channel_data['channel_description'],
    channel_data['playlist_id'],
    channel_data['subscription_count'],
    channel_data['channel_publishedAt']
)

# Execute the insert query with data
cursor.execute(insert_query, data)
mydb.commit()

# Define the insert query for Playlist Table
insert_query = "INSERT INTO Playlist VALUES (%s, %s, %s)"

# Get channel data
playlist_data = playlist_data(c_id)

data = (
    playlist_data['playlist_id'],
    playlist_data['channel_id'],
    playlist_data['playlist_name']
)

# Execute the insert query with data
cursor.execute(insert_query, data)
mydb.commit()

# Define the insert query into video table
insert_query = '''
    INSERT INTO Video 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''

# Get channel data
video_data = videos_data(c_id)

for i in range(len(video_data)):
    data = (
        video_data[i]['video_id'],
        video_data[i]['channel_id'],
        video_data[i]['video_name'],
        video_data[i]['video_description'],
        video_data[i]['video_publishedat'],
        video_data[i]['view_count'],
        video_data[i]['like_count'],
        video_data[i]['favoriteCount'],
        video_data[i]['commentCount'],
        video_data[i]['duration'],
        video_data[i]['default_thumbnails'],
        video_data[i]['caption_status']
        )
    
        # Execute the insert query with data
    cursor.execute(insert_query, data)
    mydb.commit()

# Define the insert query into comment
insert_query = '''
    INSERT INTO comment 
    VALUES (%s, %s, %s, %s, %s)
'''

# Get channel data
comment_data = comment_table

for i in range(len(comment_data)):
    data = (
        comment_data[i]['comment_id'],
        comment_data[i]['video_id'],
        comment_data[i]['comment_text'],
        comment_data[i]['author_name'],
        comment_data[i]['comment_publishedAt']
     )
    
    # Execute the insert query with data
    cursor.execute(insert_query, data)
    mydb.commit()
