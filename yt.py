import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import pymongo
from googleapiclient.discovery import build
import psycopg2

# page navigations
st.set_page_config(layout= "wide")

# option menu setup
with st.sidebar:
    selected = option_menu(None, ["Home","Extract & Transform","View"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "30px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#C80101"}})

# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.youtube_data

# connection with postgreSQL

y_db= psycopg2.connect(host="localhost",
                        user="postgres",
                        password="1234",
                        database="youtube_data_harvesting",
                        port="5432")
mycursor= y_db.cursor()

# connection with google API to get developer key
api_key = "AIzaSyBxTv0RPIz_Djvk9SRfMCTO3iu_7YsMjrQ"
youtube = build('youtube','v3',developerKey=api_key)


#  CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id = channel_id[i],
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    Country = response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data


# VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


# VIDEO DETAILS
def get_video_details(v_ids):
    video_stats = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics'].get('viewCount'),
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics'].get('favoriteCount')
                               
                               )
            video_stats.append(video_details)
    return video_stats


# COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['snippet']['topLevelComment']['id'],
                            Video_id = cmt['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt']
                            
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data


# CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name


# streamlit interface
# home page
if selected == "Home":
    st.image("https://onetechspot.com/wp-content/uploads/2021/12/1-10.png", caption="Welcome to the YouTube Data extraction application!", use_column_width=True)    
    st.title(":red[EXTRACT---->TRANSFORM---->ANALYSE]")
    
    
# extract and transaform page
if selected == "Extract & Transform":
    tab1,tab2 = st.tabs(["$\huge  EXTRACT $", "$\huge TRANSFORM $"])
    
    # tab to extract
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')

        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
            

        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                ch_details = get_channel_details(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)
                
                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d+= get_comments_details(i)
                    return com_d
                comm_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                collections3.insert_many(comm_details)
                st.success("Upload to MogoDB successful !!")
      
    # tab to transform
    with tab2:     
        st.markdown("   ")
        st.markdown("### Select a channel to begin Transformation to SQL")
        
        ch_names = channel_names()
        user_inp = st.selectbox("Select channel",options= ch_names)
        
        def insert_into_channels():
                collections = db.channel_details
                query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
                
                for i in collections.find({"Channel_name" : user_inp},{'_id' : 0}):
                    mycursor.execute(query,tuple(i.values()))
                    y_db.commit()
                
        def insert_into_videos():
            collections1 = db.video_details
            query1 = """INSERT INTO video VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in collections1.find({"Channel_name" : user_inp},{'_id' : 0}):
                mycursor.execute(query1,tuple(i.values()))
                y_db.commit()

        def insert_into_comments():
            collections1 = db.video_details
            collections2 = db.comments_details
            query2 = """INSERT INTO comments_tab VALUES(%s,%s,%s,%s,%s)"""

            for vid in collections1.find({"Channel_name" : user_inp},{'_id' : 0}):
                
                for i in collections2.find({'Video_id': vid['Video_id']},{'_id' : 0}):
                    
                    mycursor.execute(query2,tuple(i.values()))
                    y_db.commit()
                 
        if st.button("Submit"):
            try:
                
                insert_into_channels()
                insert_into_videos()
                insert_into_comments()
                st.success("Transformation to MySQL Successful !!")
            except:
                st.error("Channel details already transformed !!")
            
# VIEW PAGE
if selected == "View":
    
    st.write("## Select any question to get Insights")
    questions = st.selectbox('Questions',
    ['1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes  for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT Title AS Video_Title, Channel_name AS Channel_Name
                            FROM video
                            ORDER BY Channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=['Video_Title','Channel_Name'])
        st.write(df)
        
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT Channel_name AS Channel_Name, Total_videos AS Total_Videos
                            FROM channels
                            ORDER BY Total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=['Channel_Name','Total_Videos'])
        st.write(df)
        
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views_count AS Views 
                            FROM video
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=["Channel_Name","Video_Title","Views"])
        st.write(df)
        
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute('''select  title as video_name,comments_count as total_comments from video 
                        where comments_count is not null
                        order by title''')
        df = pd.DataFrame(mycursor.fetchall(),columns=['video_name','total_comments'])
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes_count AS Likes_Count 
                            FROM video
                            ORDER BY likes_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=['Channel_Name','Video_Title','Likes_count'])
        st.write(df)
        
        
    elif questions == '6. What is the total number of likes  for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT title AS Title, likes_count AS Likes_Count
                            FROM video
                            ORDER BY likes_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=['Video_Title','Likes_Count'])
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, view_counts AS Views
                            FROM channels
                            ORDER BY view_counts DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=['Channel_Name','Views'])
        st.write(df)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, title as Video_name, published_date as published_year
                            FROM video
                            WHERE extract(year from published_date)= 2022
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=['Channel_name','Video_name','Published_year'])
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,
                        AVG((COALESCE(CAST(minutes[1] AS INTEGER), 0) * 60 + COALESCE(CAST(seconds[1] AS INTEGER), 0))/60.0) AS "Average_Video_Duration (mins)"
                      FROM (SELECT channel_name,
                            REGEXP_MATCHES(duration, '(\d+)M') AS minutes,
                            REGEXP_MATCHES(duration, '(\d+)S') AS seconds
                      FROM video
                           ) AS subquery
                         GROUP BY channel_name
                      ORDER BY AVG((COALESCE(CAST(minutes[1] AS INTEGER), 0) * 60 + COALESCE(CAST(seconds[1] AS INTEGER), 0))/60.0) DESC;
                         """)
        df = pd.DataFrame(mycursor.fetchall(),columns=['Channel_name','Average_duration(mins)'])
        st.write(df)
        
        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comments_count AS Comments
                            FROM video
                            ORDER BY comments_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=['Channel_name','Video_id','Comments_count'])
        st.write(df)
        
