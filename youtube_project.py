import streamlit as st
import pymongo
import mysql.connector
from googleapiclient.discovery import build

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongodb_db = mongodb_client["youtube_data_lake"]

# Connect to MySQL
mysql_connection = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="1234567890",
    database="youtube_data"
)
mysql_cursor = mysql_connection.cursor()

# Initialize YouTube API client
youtube_api_key = "AIzaSyCgiK4HggK4AI7N6hbyyiEIS9f3-EKHjVg"
youtube = build('youtube', 'v3', developerKey=youtube_api_key)

def fetch_channel_data(channel_id):
    # Fetch channel data using YouTube API
    channel_response = youtube.channels().list(
        part="snippet, statistics",
        id=channel_id
    ).execute()

    if channel_response["items"]:
        channel_data = channel_response["items"][0]
        channel_name = channel_data["snippet"]["title"]
        subscribers = channel_data["statistics"]["subscriberCount"]
        video_count = channel_data["statistics"]["videoCount"]
        playlist_id = get_playlist_id(channel_id)
        
        return {
            "channel_name": channel_name,
            "subscribers": subscribers,
            "video_count": video_count,
            "playlist_id": playlist_id
        }
    else:
        return None

def get_playlist_id(channel_id):
    # Fetch playlist ID using YouTube API
    playlist_response = youtube.playlists().list(
        part="snippet",
        channelId=channel_id
    ).execute()

    if playlist_response["items"]:
        return playlist_response["items"][0]["id"]
    else:
        return None

def fetch_video_data(playlist_id):
    # Fetch video data using YouTube API
    video_response = youtube.playlistItems().list(
        part="snippet, statistics",
        playlistId=playlist_id,
        maxResults=10
    ).execute()

    videos_data = []
    for video_item in video_response["items"]:
        video_id = video_item["snippet"]["resourceId"]["videoId"]
        video_title = video_item["snippet"]["title"]
        likes = video_item["statistics"]["likeCount"]
        dislikes = video_item["statistics"]["dislikeCount"]
        comments = video_item["statistics"]["commentCount"]

        videos_data.append({
            "video_id": video_id,
            "video_title": video_title,
            "likes": likes,
            "dislikes": dislikes,
            "comments": comments
        })

    return videos_data

# Streamlit App
def main():
    st.title("YouTube Data Analysis")
    
    option = st.sidebar.selectbox("Select an option:", 
                                  ("Retrieve Channel Data", "Migrate Data to SQL", "Search Data"))
    
    if option == "Retrieve Channel Data":
        channel_id = st.text_input("Enter YouTube Channel ID:")
        if st.button("Retrieve Channel Data"):
            channel_data = fetch_channel_data(channel_id)
            if channel_data:
                st.write("Channel Name:", channel_data["channel_name"])
                st.write("Subscribers:", channel_data["subscribers"])
                st.write("Video Count:", channel_data["video_count"])
                st.write("Playlist ID:", channel_data["playlist_id"])
                if st.button("Store Data in MongoDB"):
                    mongodb_db[channel_id].insert_one(channel_data)
                    st.success("Data stored in MongoDB successfully.")
                if st.button("Store Data in SQL"):
                    mysql_cursor.execute("""
                        CREATE TABLE IF NOT EXISTS channel_data (
                            channel_id VARCHAR(255) PRIMARY KEY,
                            channel_name VARCHAR(255),
                            subscribers INT,
                            video_count INT,
                            playlist_id VARCHAR(255)
                        )
                    """)
                    mysql_cursor.execute("""
                        INSERT INTO channel_data (channel_id, channel_name, subscribers, video_count, playlist_id)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (channel_id, channel_data["channel_name"], channel_data["subscribers"], 
                          channel_data["video_count"], channel_data["playlist_id"]))
                    mysql_connection.commit()
                    st.success("Data stored in SQL database successfully.")
            else:
                st.error("Invalid Channel ID. Please try again.")

    elif option == "Migrate Data to SQL":
        channels = mongodb_db.list_collection_names()
        channel_name = st.selectbox("Select Channel:", channels)
        if st.button("Migrate Data"):
            channel_data = mongodb_db[channel_name].find_one()
            if channel_data:
                mysql_cursor.execute("""
                    CREATE TABLE IF NOT EXISTS video_data (
                        video_id VARCHAR(255) PRIMARY KEY,
                        video_title VARCHAR(255),
                        likes INT,
                        dislikes INT,
                        comments INT
                    )
                """)
                for video in fetch_video_data(channel_data["playlist_id"]):
                    mysql_cursor.execute("""
                        INSERT INTO video_data (video_id, video_title, likes, dislikes, comments)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (video["video_id"], video["video_title"], video["likes"], video["dislikes"], video["comments"]))
                mysql_connection.commit()
                st.success("Data migrated to SQL database successfully.")
            else:
                st.error("Channel Data not found. Please retrieve channel data first.")

    elif option == "Search Data":
        mysql_cursor.execute("SELECT * FROM channel_data")
        channel_data = mysql_cursor.fetchall()
        mysql_cursor.execute("SELECT * FROM video_data")
        video_data = mysql_cursor.fetchall()

        if channel_data and video_data:
            st.write("**Channel Data**")
            st.write(channel_data)

            st.write("**Video Data**")
            st.write(video_data)

            st.write("**Joining Tables**")
            mysql_cursor.execute("""
                SELECT channel_data.channel_name, video_data.video_title, video_data.likes
                FROM channel_data
                INNER JOIN video_data ON channel_data.playlist_id = video_data.playlist_id
            """)
            joined_data = mysql_cursor.fetchall()
            st.write(joined_data)
        else:
            st.error("No data found in SQL database.")

if __name__ == "__main__":
    main()
