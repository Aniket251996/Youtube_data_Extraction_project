# If we need to pull data from youtube API
import googleapiclient.discovery
import streamlit as st
import mysql.connector
mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Root',
    database='project'
    )

cursor = mydb.cursor()

st.title(':blue[Youtube API Project]')
channel_id = st.text_input("Enter your channel ID")
if st.button('Enter') and channel_id:

    api_service_name = "youtube"
    api_version = "v3"
    api_key = "AIzaSyCkU5oY4H8ftvLBo8utsvCTtICNhlv4ndY"  # Replace this with your actual API key
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    c_id = channel_id
    #c_id = "UC1tJwgblLFk3qFttkWwLr1Q"

    import mysql.connector

    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Root',
        database='project'
    )

    cursor = mydb.cursor()

    # Function to check if data is available
    def check_dta(c_id):
        query = """ 
            SELECT * FROM channel WHERE channel_id = %s
        """
        cursor.execute(query, (c_id,))
        data = cursor.fetchall()
        if len(data) > 0:
            a = len(data)
            print("This channel Id is present in DB")
        else:
            a = 0
        return a

    # Check if data is available
    if check_dta(c_id) == 0:
        # Pull Channel Data
        def channel_data(c_id):
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=c_id
            )
            response = request.execute()
            data = {"channel_id": response['items'][0]['id'],
                    "channel_name": response['items'][0]['snippet']['title'],
                    "channel_view": response['items'][0]['statistics']['viewCount'],
                    "channel_description": response['items'][0]['snippet']['description'],
                    "channel_publishedAt": response['items'][0]['snippet']['publishedAt'],
                    "playlist_id": response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                    "subscription_count": response['items'][0]['statistics']['subscriberCount']
                    }
            return data

        # Pull Playlist Data
        def playlist_data(c_id):
            request = youtube.playlists().list(
                part="snippet",
                channelId=c_id
            )
            response = request.execute()

            if 'items' in response and len(response['items']) > 0:
                # If playlists are found, extract the data for the first playlist
                data = {
                    "playlist_id": response['items'][0]['id'],
                    "channel_id": response['items'][0]['snippet']['channelId'],
                    "playlist_name": response['items'][0]['snippet']['title']
                }
                return data
            else:
                # If no playlists are found, return None
                return None

        # Pull Video Data
        search_request = youtube.search().list(
            part="id",
            channelId=c_id,
            type="video"
        )
        search_response = search_request.execute()

        video_ids = [item['id']['videoId'] for item in search_response['items']]

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

        # Pull Comment Data
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
                        'comment_text': item['snippet']['topLevelComment']['snippet']['textOriginal'],
                        'author_name': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'comment_publishedAt': item['snippet']['topLevelComment']['snippet']['publishedAt']
                    })

        # Insert Data into Tables
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

        # Get playlist data
        playlist_data = playlist_data(c_id)

        data = (
            playlist_data['playlist_id'],
            playlist_data['channel_id'],
            playlist_data['playlist_name']
        )

        # Execute the insert query with data
        cursor.execute(insert_query, data)
        mydb.commit()

        # Define the insert query for video table
        insert_query = '''
            INSERT INTO Video 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''

        # Get video data
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

        # Define the insert query for comment
        insert_query = '''
            INSERT INTO comment 
            VALUES (%s, %s, %s, %s, %s)
        '''

        # Get comment data
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

    # Data to display in streamlit application

#function to show basic channel related data for the given Channel Id passed by user
def show_basic_data():
    comments = "Here is the details for the requested Channel ID: "
    st.write(comments, channel_id)

    # Write SQL queries to retrieve data
    channel_query = "SELECT * FROM channel WHERE channel_id = %s"

    # Execute the queries with the provided channel ID
    cursor.execute(channel_query, (channel_id,))
    channel_data = cursor.fetchone()

    # Display the fetched data
    if channel_data:
        st.header("Channel Information")
        st.write("Channel Name:", channel_data[1])
        st.write("Channel Description:", channel_data[3])
        st.write("Channel View Count:", channel_data[2])
        st.write("Subscription Count:", channel_data[5])
        query = """SELECT count(*) FROM video WHERE channel_id = %s"""
        cursor.execute(query, (channel_id,))
        count_result = cursor.fetchone()[0]  # Fetch the count and access the first element of the tuple
        st.write("No of video: ", count_result)
        query = """SELECT count(*) FROM comment WHERE video_id in (select video_id from video where channel_id = %s)"""
        cursor.execute(query, (channel_id,))
        count_result = cursor.fetchone()[0]  # Fetch the count and access the first element of the tuple
        st.write("No of comment: ", count_result)
        video_query = "SELECT * FROM video WHERE channel_id = %s limit 1"
        cursor.execute(video_query, (channel_id,))
        video_data = cursor.fetchall()
        if video_data:
            for video in video_data:
                default_thumbnail_url = video[10]
                if default_thumbnail_url:
                    st.write("Default Thumbnail:")
                    # Increase the size of the image by specifying the width
                    st.image(default_thumbnail_url, caption='Default Thumbnail', width=400)
                else:
                    st.write("No Default Thumbnail available for this video.")


        # Close the cursor and connection
        cursor.close()
        mydb.close()

if st.button('Click here to see Channel Basic details'):
    show_basic_data()
