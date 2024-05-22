# Streamlit App Developement
import streamlit as st
import mysql.connector
import re

# Function to execute SQL query and display results based on user selection
def execute_query(option):
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Root',
        database='project'
    )
    cursor = mydb.cursor()
#question 1
    if option == "What are the names of all the videos and their corresponding channels?":
        query = """select v.vedio_name,c.channel_name from project.video as v
                    inner join project.channel as c on v.channel_id = c.channel_id 
                """
        
        cursor.execute(query)
        results = cursor.fetchall()
        st.write("What are the names of all the videos and their corresponding channels?")
        if results:
            columns = ["Video Name", "Channel Name"]
            results = [(str(row[0]), str(row[1])) for row in results]
            st.table([columns] + results)
        else:
            st.write("No results found.")
#question2    
    if option == "Which channels have the most number of videos, and how many videos do they have?":
        query = """SELECT v.channel_id, c.channel_name, COUNT(vedio_name) AS video_count 
                FROM video as v
                INNER JOIN channel as c  ON v.channel_id = c.channel_id 
                GROUP BY v.channel_id ,c.channel_name
                ORDER BY video_count DESC 
                limit 1
                """
        cursor.execute(query)
        results = cursor.fetchall()
        st.write("Which channels have the most number of videos, and how many videos do they have?")
        if results:
            columns = ["Channel ID", "Channel Name", "Video Count"]
            results = [(str(row[0]), str(row[1]), str(row[2])) for row in results]
            st.table([columns] + results)
        else:
            st.write("No results found.")
#question3
    if option == "What are the top 10 most viewed videos and their respective channels?":
        query = """select v.vedio_name,v.view_count, c.channel_name from video as v
                    inner join channel as c on v.channel_id = c.channel_id 
                    order by view_count desc
                    limit 10;
                """
        cursor.execute(query)
        results = cursor.fetchall()
        st.write("What are the top 10 most viewed videos and their respective channels?")
        if results:
            columns = ["video name","No of view", "Channel Name"]
            results = [(str(row[0]), str(row[1]),str(row[2])) for row in results]
            st.table([columns] + results)
        else:
            st.write("No results found.")
#queation 4
    if option == "How many comments were made on each video, and what are their corresponding video names?":
        query = """select video_id as video_id, vedio_name as video_name, commentCount as comment_count from video
                """
        cursor.execute(query)
        results = cursor.fetchall()
        st.write("How many comments were made on each video, and what are their corresponding video names?")
        if results:
            columns = ["Video ID", "Video Name","Comment Count"]
            results = [(str(row[0]), str(row[1]),str(row[2])) for row in results]
            st.table([columns] + results)
        else:
            st.write("No results found.")
#queation 5
    if option == "Which videos have the highest number of likes, and what are their corresponding channel names?":
        query = """select v.vedio_name as video_name , c.channel_name as channel_name from video as v
                    inner join channel as c on v.channel_id = v.channel_id
                    order by like_count desc
                    limit 1;
                """
        cursor.execute(query)
        results = cursor.fetchall()
        st.write("Which videos have the highest number of likes, and what are their corresponding channel names?")
        if results:
            columns = ["Video Name", "Channel Name"]
            results = [(str(row[0]), str(row[1])) for row in results]
            st.table([columns] + results)
        else:
            st.write("No results found.")
#question 6
    if option == "What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query = """select vedio_name as video_name , like_count as No_of_like from video
                """
        cursor.execute(query)
        results = cursor.fetchall()
        st.write("What is the total number of likes and dislikes for each video, and what are their corresponding video names?")
        if results:
            columns = ["Video Name", "Likes"]
            results = [(str(row[0]), str(row[1])) for row in results]
            st.table([columns] + results)
        else:
            st.write("No results found.")    
# Question 7
    if option == "What is the total number of views for each channel, and what are their corresponding channel names?":
        query = """select channel_name as Channel_name, channel_view as channel_views from channel
                """
        cursor.execute(query)
        results = cursor.fetchall()
        st.write("What is the total number of views for each channel, and what are their corresponding channel names?")
        if results:
            columns = ["channel Name", "Channel View"]
            results = [(str(row[0]), str(row[1])) for row in results]
            st.table([columns] + results)
        else:
            st.write("No results found.") 
# Question 8
    if option == "What are the names of all the channels that have published videos in the year 2022?":
        query = """select c.channel_name,v.vedio_name from video as v
                    inner join channel as c on c.channel_id = v.channel_id
                    where v.video_publishedat like '2022%'
                """
        cursor.execute(query)
        results = cursor.fetchall()
        st.write("What are the names of all the channels that have published videos in the year 2022?")
        if results:
            columns = ["Channel Name","Video Name"]
            results = [(str(row[0]),str(row[1])) for row in results]
            st.table([columns] + results)
        else:
            st.write("No results found.") 
# Question 9
    if option == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":

        query = """SELECT 
                channel_id,
                channel_name,
                SUM(extracted_data) AS total_extracted_data
            FROM (
                SELECT 
                    v.channel_id,
                    c.channel_name,
                    ROUND(AVG(
                        CASE 
                            WHEN INSTR(v.duration, 'M') > 0 
                            THEN SUBSTRING_INDEX(SUBSTRING_INDEX(v.duration, 'PT', -1), 'M', 1)
                            ELSE 0 
                        END), 2) AS extracted_data
                FROM project.video AS v
                INNER JOIN project.channel AS c ON v.channel_id = c.channel_id
                GROUP BY v.channel_id, c.channel_name
                
                UNION ALL
                
                SELECT 
                    v2.channel_id,
                    c2.channel_name,
                    ROUND((AVG(
                        CASE 
                            WHEN v2.duration LIKE 'PT%M%S' THEN 
                                SUBSTRING_INDEX(SUBSTRING_INDEX(v2.duration, 'M', -1), 'S', 1)
                            WHEN v2.duration LIKE 'PT%S' THEN
                                SUBSTRING_INDEX(SUBSTRING_INDEX(v2.duration, 'PT', -1), 'S', 1)
                            ELSE 
                                0
                        END ) / 60), 2) AS extracted_data
                FROM project.video AS v2
                INNER JOIN project.channel AS c2 ON v2.channel_id = c2.channel_id
                GROUP BY v2.channel_id, c2.channel_name
            ) AS subquery
            GROUP BY channel_id, channel_name;
        """
  
        cursor.execute(query)
        results = cursor.fetchall()
        
        st.write("What is the average duration of all videos in each channel, and what are their corresponding channel names?")
        if results:
            columns = ["Channel ID","Channel Name","Average Duration in Min"]
            results = [(str(row[0]),str(row[1]),str(row[2])) for row in results]
            st.table([columns] + results)
        else:
            st.write("No results found.")       
# Question 10
    if option == "Which videos have the highest number of comments, and what are their corresponding channel names?":
        query = """select v.vedio_name,c.channel_name, v.commentCount from project.video as v
                    inner join project.channel as c on v.channel_id = c.channel_id
                    order by v.commentCount desc
                    limit 1;
                """
        cursor.execute(query)
        results = cursor.fetchall()
        st.write("Videos with the highest number of comments and their corresponding channel names:")
        if results:
            columns = ["Video name","Channel Name", "Comment count"]
            results = [(str(row[0]), str(row[1]),str(row[2])) for row in results]
            st.table([columns] + results)
        else:
            st.write("No results found.")


# Streamlit App Title
st.title(':blue[Youtube API Project]')
#channel_id = st.text_input("Enter your channel ID")

option = st.selectbox(
    "Please select the question?",
    ("What are the names of all the videos and their corresponding channels?",
    "Which channels have the most number of videos, and how many videos do they have?",
    "What are the top 10 most viewed videos and their respective channels?",
    "How many comments were made on each video, and what are their corresponding video names?",
    "Which videos have the highest number of likes, and what are their corresponding channel names?",
    "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "What is the total number of views for each channel, and what are their corresponding channel names?",
    "What are the names of all the channels that have published videos in the year 2022?",
    "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "Which videos have the highest number of comments, and what are their corresponding channel names?"),
    index=None)
    
# Streamlit Run button
if st.button('Run Query'):
    execute_query(option)
