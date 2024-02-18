#importing package
from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import pandas as pd
import psycopg2
import streamlit as st 

#Api key connection function creation

def api_connection():
    api_key="AIzaSyBiWbxSKiqSr-ptnQ60rwJMoxytjGkMdC4"
    
    api_service_name= "youtube"
    api_version="v3"
    
    youtube= build(api_service_name,api_version,developerKey=api_key)
    return youtube

Youtube= api_connection()

#scrapping channel information
def channel_info(channel_id):
    request = Youtube.channels().list(
        
        part="snippet,contentDetails,statistics",
        id=channel_id
    )

    response= request.execute()

    #using for loop to get items
    for i in response["items"]:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers_Count=i["statistics"]["subscriberCount"],
                Channel_Views=i["statistics"]["viewCount"],
                Channel_videos_count=i["statistics"]["videoCount"],
                Channel_description= i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data
    
    
#getting all video Ids from channel
#1st step is to get playlist id

def Video_ids(channel_id):
  Video_Ids=[]                 
  response=Youtube.channels().list(id=channel_id,                            
                                  part='contentDetails').execute()
  Playlist_Id= response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  

  next_page_token=None    #to get items from next page

  while True:
    response1= Youtube.playlistItems().list(part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
    

    for i in range(len(response1['items'])):
      Video_Ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
    next_page_token=response1.get('nextPageToken')
    
    if next_page_token is None:
      break
  return Video_Ids

#to get each vedio information from channel


def Video_info(video__ids):
  Video_Data=[]
  for V_id in video__ids :
      response2= Youtube.videos().list(part='snippet,ContentDetails,statistics',
                                      id=V_id ).execute()
      
      for item in response2['items']:
          data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_id=item['snippet']['channelId'],
                    Video_id=item['id'],
                    Video_Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Video_Description=item['snippet'].get('description'),
                    Published_At=item['snippet']['publishedAt'],
                    Duration =item['contentDetails']['duration'],
                    View_Count= item['statistics'].get('viewCount'),
                    Comments_Count=item['statistics'].get('commentCount'),
                    Favourite_Count=item['statistics'].get('favouriteCount'),
                    Like_Count=item['statistics'].get('likeCount'),
                    Caption_Status=item['contentDetails']['caption']
                    
                    
                  )
          Video_Data.append(data)
  return Video_Data


#to get comment information from videos

def Comment_info(videos__ids):
    Comment_Data=[]
    try:
        for v_id in videos__ids:
            response3= Youtube.commentThreads().list(part='snippet',
                                                    videoId=v_id ,
                                                    maxResults=20).execute()
            for item in response3['items']:
                data= dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text= item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published_At=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_Data.append(data)

    except:
        pass

    return Comment_Data


#to get Playlist Details

def Playlist_info(channel_id):
    Playlist_Data=[]
    next_page_token=None

    while True:
        response3= Youtube.playlists().list(part='snippet,contentDetails',
                                            channelId=channel_id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()


        for item in response3['items']:
            data=dict(Playlist_Id=item['id'],
                    Playlist_Title=item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    Video_count=item['contentDetails']['itemCount'],
                    Published_At=item['snippet']['publishedAt'])
            Playlist_Data.append(data)
            
        next_page_token=response3.get('nextPageToken')
        if next_page_token is None:
            break
    return Playlist_Data

#connection to mongodb

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client['youtube_data_harvesting']

#creating function to create collection in the database

def Channel_Details(channel_id):
    channel_details=channel_info(channel_id)
    playlist_details=Playlist_info(channel_id)
    video_id_details=Video_ids(channel_id)
    video_details=Video_info(video_id_details)
    comment_details=Comment_info(video_id_details)
    
    
    collection=db["Channel_Details"]
    collection.insert_one({"Channel_Information":channel_details,
                           "Playlist_Information":playlist_details,
                           "Video_Information":video_details,
                           "Comment_Information":comment_details})
    global channel_names
    channel_names = retrieve_channel_names()
    return "Upload Completed in Mongodb"
    
    
# Function to retrieve channel names from MongoDB
def retrieve_channel_names():
    client = MongoClient("mongodb://localhost:27017/")
    db = client['youtube_data_harvesting']
    collection = db["Channel_Details"]
    
    channel_names = set()
    for channel_data in collection.find({}, {"_id": 0, "Channel_Information.Channel_Name": 1}):
        channel_names.add(channel_data["Channel_Information"]["Channel_Name"])

    return sorted(list(channel_names))

channel_names=retrieve_channel_names()


# Channel Table creation in postgresql
def Channels_Table():
    y_db= psycopg2.connect(host="localhost",
                        user="postgres",
                        password="1234",
                        database="youtube_data_harvesting",
                        port="5432")
    cursor= y_db.cursor()

    drop_query= '''drop table if exists channels'''
    cursor.execute(drop_query)
    y_db.commit()


    try:
        create_query='''create table if not exists channels(Channel_Name VARCHAR(100),
                                                                Channel_Id VARCHAR(80) PRIMARY KEY,
                                                                Subscribers_Count BIGINT,
                                                                Channel_Views BIGINT,
                                                                Channel_videos_count INT,
                                                                Channel_description TEXT,
                                                                Playlist_Id VARCHAR(80))'''
        cursor.execute(create_query)
        y_db.commit()
        
    except:
        print("channels table created")
            
        
    channel_list=[]
    db=client['youtube_data_harvesting']
    collection=db["Channel_Details"]
        
    for channel_data in collection.find({},{"_id":0,"Channel_Information":1}):
        channel_list.append(channel_data["Channel_Information"])
        
    df= pd.DataFrame(channel_list)


    for index,row in df.iterrows():
        
            insert_query=''' INSERT into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscribers_Count,
                                                    Channel_Views  ,
                                                    Channel_videos_count,
                                                    Channel_description,
                                                    Playlist_Id)
                                                    
                                                    
                                                    values(%s,%s,%s,%s,%s,%s,%s)'''
            Values=(row['Channel_Name'],
                        row['Channel_Id'],
                        row['Subscribers_Count'],
                        row['Channel_Views'],
                        row['Channel_videos_count'],
                        row['Channel_description'],
                        row['Playlist_Id'])
            
            try:
                cursor.execute(insert_query,Values)
                y_db.commit()
            
            except:
                st.write("channels are inserted in table")
        

#playlist table

def Playlists_Table():
    y_db= psycopg2.connect(host="localhost",
                        user="postgres",
                        password="1234",
                        database="youtube_data_harvesting",
                        port="5432")
    cursor= y_db.cursor()

    drop_query= '''drop table if exists playlists'''
    cursor.execute(drop_query)
    y_db.commit()

    Create_Query = """Create table if not exists playlists(Playlist_Id VARCHAR(100) PRIMARY KEY,
                                                            Playlist_Title VARCHAR(100) ,
                                                            Channel_Id VARCHAR(80),
                                                            Channel_Name VARCHAR(100),
                                                            Video_count INT,
                                                            Published_At timestamp )"""
    cursor.execute(Create_Query )
    y_db.commit()

    Playlist_list=[]
    db=client['youtube_data_harvesting']
    collection=db["Channel_Details"]
    for playlist_data in collection.find({},{"_id":0,"Playlist_Information":1}):
        for i in range(len(playlist_data["Playlist_Information"])):
            Playlist_list.append(playlist_data["Playlist_Information"][i])
            
    df1= pd.DataFrame(Playlist_list)

    for index,row in df1.iterrows():
                Insert_Query=''' INSERT into playlists(Playlist_Id,
                                                    Playlist_Title,
                                                    Channel_Id,
                                                    Channel_Name ,
                                                    Video_count,
                                                    Published_At)
                                                    
                                                    
                                                    values(%s,%s,%s,%s,%s,%s);'''
                Values=(row['Playlist_Id'],
                        row['Playlist_Title'],
                        row['Channel_Id'],
                        row['Channel_Name'],
                        row['Video_count'],
                        row['Published_At'],
                        )
            
                try:
                    cursor.execute(Insert_Query,Values)
                    y_db.commit()
                    
                except:
                    st.write("playlists are inserted in table")
                    
                    
#vedio table
def Videos_Table():
  y_db= psycopg2.connect(host="localhost",
                      user="postgres",
                      password="1234",
                      database="youtube_data_harvesting",
                      port="5432")
  cursor= y_db.cursor()

  drop_query= '''drop table if exists videos'''
  cursor.execute(drop_query)
  y_db.commit()

  Create_Query = """Create table if not exists videos(Channel_Name VARCHAR(100),
                                                      Channel_id VARCHAR(80),
                                                      Video_id VARCHAR(80) PRIMARY KEY,
                                                      Video_Title VARCHAR(150),
                                                      Tags TEXT,
                                                      Thumbnail VARCHAR(200),
                                                      Video_Description TEXT,
                                                      Published_At timestamp,
                                                      Duration interval,
                                                      View_Count BIGINT,
                                                      Comments_Count INT,
                                                      Favourite_Count INT,
                                                      Like_Count BIGINT,
                                                      Caption_Status VARCHAR(50)
                                                      )"""
  cursor.execute(Create_Query )
  y_db.commit()


  Video_list=[]
  db=client['youtube_data_harvesting']
  collection=db["Channel_Details"]
  for video_data in collection.find({},{"_id":0,"Video_Information":1}):
      for i in range(len(video_data["Video_Information"])):
          Video_list.append(video_data["Video_Information"][i])
          
  df3= pd.DataFrame(Video_list)


  for index,row in df3.iterrows():
              Insert_Query=''' INSERT into videos(Channel_Name,
                                                  Channel_id,
                                                  Video_id,
                                                  Video_Title,
                                                  Tags,
                                                  Thumbnail,
                                                  Video_Description,
                                                  Published_At,
                                                  Duration,
                                                  View_Count,
                                                  Comments_Count,
                                                  Favourite_Count,
                                                  Like_Count,
                                                  Caption_Status 
              
                                                    )
                                                  
                                                  
                                                  values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
              Values=(row['Channel_Name'],
                      row['Channel_id'],
                      row['Video_id'],
                      row['Video_Title'],
                      row['Tags'],
                      row['Thumbnail'],
                      row['Video_Description'],
                      row['Published_At'],
                      row['Duration'],
                      row['View_Count'],
                      row['Comments_Count'],
                      row['Favourite_Count'],
                      row['Like_Count'],
                      row['Caption_Status']
                      
                      )
          
              try:
                  cursor.execute(Insert_Query,Values)
                  y_db.commit()
                  
              except:
                  st.write("videos are inserted in table")
                  
#comment table
def Comments_Table():

  y_db= psycopg2.connect(host="localhost",
                      user="postgres",
                      password="1234",
                      database="youtube_data_harvesting",
                      port="5432")
  cursor= y_db.cursor()

  drop_query= '''drop table if exists comments'''
  cursor.execute(drop_query)
  y_db.commit()

  Create_Query = """Create table if not exists comments(Comment_Id VARCHAR(80) PRIMARY KEY,
                                                      Video_Id VARCHAR(80),
                                                      Comment_Text TEXT,
                                                      Comment_Author VARCHAR(200),
                                                      Comment_Published_At timestamp
                                                      )"""
  cursor.execute(Create_Query )
  y_db.commit()


  Comment_list=[]
  db=client['youtube_data_harvesting']
  collection=db["Channel_Details"]
  for comment_data in collection.find({},{"_id":0,"Comment_Information":1}):
      for i in range(len(comment_data["Comment_Information"])):
          Comment_list.append(comment_data["Comment_Information"][i])
          
  df4= pd.DataFrame(Comment_list)

  for index,row in df4.iterrows():
              Insert_Query=''' INSERT into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published_At
                                                                                                    
                                                    )
                                                  
                                                  
                                                  values(%s,%s,%s,%s,%s);'''
              Values=(row['Comment_Id'],
                      row['Video_Id'],
                      row['Comment_Text'],
                      row['Comment_Author'],
                      row['Comment_Published_At']
                      
                      
                      )
          
              try:
                cursor.execute(Insert_Query,Values)
                y_db.commit()
                  
              except:
                st.write("comments are inserted in table")

#defining the function for tables

def Tables():
    Channels_Table()
    Playlists_Table()
    Videos_Table()
    Comments_Table()
    return "Migrated successfully!"



#creating UI using streamlit
# Function to navigate to different pages
def page_navigation():
    
    selected_page = st.sidebar.selectbox("Select Page", ["Home", "Extract and Transform", "View"])

    # Using conditional statements to display the page
    if selected_page == "Home":
        st.image("https://onetechspot.com/wp-content/uploads/2021/12/1-10.png", caption="Welcome to the YouTube Data extraction application!", use_column_width=True)    
        st.title(":red[EXTRACT---->TRANSFORM---->ANALYSE]")
    elif selected_page == "Extract and Transform":
        st.header(":green[Extract and Transform YouTube Data]")
        st.caption("Hint: Go to channel's home page>> Right click>> view page source>> Find channel_id")    
        channel_id = st.text_input("ENTER YOUR CHANNEL ID BELOW")
        if st.button("Extract & upload to mongodb"):
            ch_ids = []
            db = client['youtube_data_harvesting']
            collection = db["Channel_Details"]
            for ch_data in collection.find({}, {"_id": 0, "Channel_Information": 1}):
                ch_ids.append(ch_data["Channel_Information"]["Channel_Id"])
            if channel_id in ch_ids:
                st.success("Given channel id already exists")
            else:
                inserted_dictionary = Channel_Details(channel_id)
                st.success(inserted_dictionary)
        selected_channel = st.selectbox("SELECT THE CHANNEL BELOW", channel_names)  
    
        if st.button("Migrate to SQL"):
            table=Tables()
            st.success(table)
    elif selected_page == "View":
        st.header(":green[Insights from Data]")
        y_db= psycopg2.connect(host="localhost",
                    user="postgres",
                    password="1234",
                    database="youtube_data_harvesting",
                    port="5432")
        cursor= y_db.cursor()

        questions=st.selectbox("Select Your question below",("1.What are the names of all the videos and their corresponding channels?",
                                                                "2.Which channels have the most number of videos, and how many videos do they have?",
                                                                "3. What are the top 10 most viewed videos and their respective channels?",
                                                                "4. How many comments were made on each video, and what are their corresponding video names?",
                                                                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                                "6. What is the total number of likes  for each video, and what are their corresponding video names?",
                                                                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                                "8. What are the names of all the channels that have published videos in the year 2022",
                                                                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

        if  questions=="1.What are the names of all the videos and their corresponding channels?":
            Query1='''select video_title as videosname,channel_name as channelname from videos'''
            cursor.execute(Query1)
            y_db.commit()
            table1=cursor.fetchall()
            df1=pd.DataFrame(table1,columns=["Video Name","channel Name"])
            st.write(df1)
            
        elif questions=="2.Which channels have the most number of videos, and how many videos do they have?":
            Query2='''select channel_name as channelsname,channel_videos_count as total_video_count from channels 
                        order by channel_videos_count desc'''
            cursor.execute(Query2)
            y_db.commit()
            table2=cursor.fetchall()
            df2=pd.DataFrame(table2,columns=["Channel Name","Number of Videos"])
            st.write(df2)
            
        elif questions=="3. What are the top 10 most viewed videos and their respective channels?":
            Query3='''select channel_name as channel_title, video_title as video_name,view_count as views from videos
                        where view_count is not null order by view_count desc limit 10'''
            cursor.execute(Query3)
            y_db.commit()
            table3=cursor.fetchall()
            df3=pd.DataFrame(table3,columns=["Channel Name","Video Name","Views"])
            st.write(df3)
            
        elif questions=="4. How many comments were made on each video, and what are their corresponding video names?":
            Query4='''select  video_title as video_name,comments_count as total_comments from videos 
                        where comments_count is not null'''
            cursor.execute(Query4)
            y_db.commit()
            table4=cursor.fetchall()
            df4=pd.DataFrame(table4,columns=["Video Name","Comments Count"])
            st.write(df4)
            
        elif questions=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
            Query5='''select video_title as video_name, channel_name as channel_title, like_count as total_likes from videos 
                        where like_count is not null order by like_count desc'''
            cursor.execute(Query5)
            y_db.commit()
            table5=cursor.fetchall()
            df5=pd.DataFrame(table5,columns=["Video Name","Channel Name","Max Likes"])
            st.write(df5)

        elif questions=="6. What is the total number of likes  for each video, and what are their corresponding video names?":
            Query6='''select video_title as video_name, like_count as total_likes from videos '''
            cursor.execute(Query6)
            y_db.commit()
            table6=cursor.fetchall()
            df6=pd.DataFrame(table6,columns=["Video Name","Total Likes"])
            st.write(df6)
            
        elif questions=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
            Query7='''select channel_name as channel_title,channel_views as views from channels'''
            cursor.execute(Query7)
            y_db.commit()
            table7=cursor.fetchall()
            df7=pd.DataFrame(table7,columns=["Channel Name","Total Views"])
            st.write(df7)
            
        elif questions=="8. What are the names of all the channels that have published videos in the year 2022":
            Query8='''select channel_name as channel_title,video_title as vedio_name, published_at as video_released from videos
                        where extract(year from published_at)= 2022'''
            cursor.execute(Query8)
            y_db.commit()
            table8=cursor.fetchall()
            df8=pd.DataFrame(table8,columns=["Channel Name","Video Name","Release Year"])
            st.write(df8)
            
        elif questions=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            Query9='''select channel_name as channel_title, AVG(duration) as average_duration from videos
                        group by channel_name'''
            cursor.execute(Query9)
            y_db.commit()
            table9=cursor.fetchall()
            df9=pd.DataFrame(table9,columns=["Channel Name","Average Duration"])
            
            
        #to insert average duration in string format in streamlit
            TABLE_9=[]
            for index,row in df9.iterrows():
                channel_name=row["Channel Name"]
                average_duration=row["Average Duration"]
                average_duration_in_string=str(average_duration)
                TABLE_9.append(dict(ChannelTitle=channel_name,Average_Duration=average_duration_in_string))
            df=pd.DataFrame(TABLE_9)
            st.write(df)

        elif questions=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
            Query10='''select channel_name as channel_title, video_title as video_names ,comments_count as comments from videos
                        where comments_count is not null order by comments desc'''
            cursor.execute(Query10)
            y_db.commit()
            table10=cursor.fetchall()
            df10=pd.DataFrame(table10,columns=["Channel Name","Video Name","Comments Count"])
            st.write(df10)
        
page_navigation()

