import mysql.connector as sql
import pymongo
import googleapiclient.discovery
from pprint import pprint
import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import plotly.express as px
from PIL import Image


api_key = "AIzaSyBLxlbVy1IFe0SOD7tr-YvsBf95qeDY3dE"

youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

#DEFINING FUNCTION TO GET CHANNEL INFORMATION
def get_channel_info(c_id):
  request = youtube.channels().list(part="snippet,contentDetails,statistics",
        id = c_id)
  response = request.execute()
  chan_data = dict( chan_id = response['items'][0]['id'], 
            chan_name = response['items'][0]['snippet']['title'],
            chan_type = response['items'][0]['kind'],
            chan_views = response['items'][0]['statistics']['viewCount'],
            chan_desc = response['items'][0]['snippet']['description'],
            chan_status = response['items'][0]['snippet']['publishedAt'],
            playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            total_videos = response['items'][0]['statistics']['videoCount'])
  
  return chan_data

#DEFINING FUNCTION TO GET THE IDs OF VIDEOS PRESENT IN THE CHANNEL
def get_channel_videos(c_id):
    video_ids = []
    vid_req = youtube.channels().list(id=c_id, 
                                  part='contentDetails')
    vid_res = vid_req.execute()
    playlist_id = vid_res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=10,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
            
   
        if next_page_token is None:
            break

    return video_ids

#DEFINING FUNCTION TO GET EACH VIDEO DETAILS 
def get_video_details(v_ids):
    video_data = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(chan_name = video['snippet']['channelTitle'],
                                chan_id = video['snippet']['channelId'], 
                                Video_id = video['id'],
                                Video_name = video['snippet']['title'],
                                Video_Desc = video['snippet']['description'],
                                Pub_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views_count = video['statistics']['viewCount'],
                                Likes_count = video['statistics'].get('likeCount'),
                                Comments_count = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_data.append(video_details)

    return video_data

# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId = v_id,
                                                    maxResults = 10,
                                                    pageToken = next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    
    return comment_data

#CONNECTING TO MongoDB AND CREATING A DATABASE
client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['testbench_youtube']
collection = db['collection_db']

# CONNECTING WITH MYSQL DATABASE
mydb = sql.connect(host="localhost",
                   user="root",
                   password="Rohan@sql04",
                   database= "youtube_db",
                   use_pure = True
                  )
mycursor = mydb.cursor()

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['chan_name'])
    #print(ch_name)
    return ch_name

# SETTING PAGE CONFIGURATIONS
icon = Image.open("youtube_logo.png")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing",
                   page_icon= icon,
                   layout= "wide",
                   initial_sidebar_state= "expanded")

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Extract and Transform","View"], 
                           icons=["house-door-fill","tools","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#008000"},
                                   "icon": {"font-size": "20px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#01C1C8"}})

# HOME PAGE
if selected == "Home":
    # Title Image
    
    col1,col2 = st.columns(2,gap= 'medium')
    col1.markdown("### :black[Domain] :blue[Social Media]")
    col1.markdown("### :black[Technologies used] :blue[Python, MongoDB, Youtube Data API, MySql, Streamlit]")
    col1.markdown("### :black[Overview] :blue[Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.]")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.markdown("#   ")

# EXTRACT, LOAD and TRANSFORM PAGE
if selected == "Extract and Transform":
    tab1,tab2 = st.tabs(["$\huge EXTRACT $", "$\huge TRANSFORM $"])
    
    # EXTRACT INFO AND LOAD INTO MONGODB TAB
    with tab1:
        st.markdown("#    ")
        st.write("## Enter the YouTube Channel ID below :")
        ch_id = st.text_input("Hint : Go to channel's home page > Click on About > Click share button > Click on Copy channel id").split(',')

        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_info(ch_id)
            st.write(f'## Extracted data from :green["{ch_details["chan_name"]}"] channel')
            st.table(ch_details)

        if st.button("Upload to MongoDB"):
            with st.spinner('Uploading the data....'):
                ch_details = get_channel_info(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)
                
                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d+= get_comments_details(i)
                    return com_d
                comm_details = comments()

# TAKE THE CHANNEL DETAILS AND LOAD INTO A COLLECTION IN MONGODB DATABASE
                collections1 = db.channel_details
                collections1.insert_one(ch_details)

# TAKE THE VIDEO DETAILS AND LOAD INTO A COLLECTION IN MONGODB DATABASE
                collections2 = db.video_details
                collections2.insert_many(vid_details)

# TAKE THE COMMENT DETAILS AND LOAD INTO A COLLECTION IN MONGODB DATABASE
                collections3 = db.comments_details
                collections3.insert_many(comm_details)
                st.success("Upload to MogoDB successful !!")

      
    # TRANSFORM TAB
    with tab2:     
        st.markdown("#   ")
        st.markdown("### Select the channel to Transform to SQL")
        
        ch_names = channel_names()
        user_inp = st.selectbox("Select channel",options= ch_names)
        
        def insert_into_channels():
                collections = db.channel_details
                query = """INSERT INTO Channels (channel_id, channel_name, channel_type, channel_views, channel_description, channel_status, playlist_id, total_videos )
                     VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
                
                for i in collections.find({"chan_name" : user_inp},{'_id':0}):
                    mycursor.execute(query,tuple(i.values()))
                    mydb.commit()
                
        def insert_into_videos():
            vid_col = db["video_details"]
            query1 = """INSERT INTO Videos (channel_name, channel_id, video_id, video_name, 
            video_description, published_date, duration, view_count, like_count, 
            comment_count, favorite_count, caption_status)
             VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in vid_col.find({"chan_name" : user_inp},{"_id":0}):
                mycursor.execute(query1,tuple(i.values()))
                mydb.commit()

        def insert_into_comments():
            vid_col1 = db.video_details
            com_col = db.comments_details
            query2 = """INSERT INTO Comment (comment_id, video_id, comment_text, comment_author, comment_posted_date, like_count, reply_count)
              VALUES(%s,%s,%s,%s,%s,%s,%s)"""

            for vid in vid_col1.find({"chan_name" : user_inp},{'_id' : 0}):
                for i in com_col.find({'Video_id': vid['Video_id']},{'_id' : 0}):
                    t=tuple(i.values())
                    mycursor.execute(query2,t)
                    mydb.commit()

        def insert_into_playlist():
            pl_col = db["playlist_details"]
            query1 = """INSERT INTO Playlist (playlist_id, channel_id, playlist_name)
             VALUES(%s,%s,%s)"""

            for i in pl_col.find({"chan_name" : user_inp},{"_id":0}):
                mycursor.execute(query1,tuple(i.values()))
                mydb.commit()


        if user_inp and st.button("Submit"):

            with st.spinner('Tranforming into SQL Database....'):

#EXECUTING COMMAND TO TRUNCATE THE TABLE DATA IN SQL DATABASE                
                a1 = """TRUNCATE TABLE Channels"""
                a2 = """TRUNCATE TABLE Playlist"""
                a3 = """TRUNCATE TABLE Comment"""
                a4 = """TRUNCATE TABLE Videos"""
                mycursor.execute(a1)
                mydb.commit()
                mycursor.execute(a2)
                mydb.commit()
                mycursor.execute(a3)
                mydb.commit()
                mycursor.execute(a4)
                mydb.commit()

#CALLING THE FUNCTIONS TO RETRIEVE INFO FROM MONGODB AND LOAD INTO SQL DATABASE                
                insert_into_channels()
                insert_into_videos()
                insert_into_comments()
                insert_into_playlist()
                st.success("Transformation to MySQL Successful!!!")
                

# ANALYSING THE DATA IN SQL DATABASE PAGE 
if selected == "Analyse":
    
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

#Implement the sql instructions for every question selected to get the desired result    

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT video_name AS Video_Title, channel_name AS Channel_Name
            FROM Videos
            ORDER BY channel_name"""
            )
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name 
        AS Channel_Name, total_videos AS Total_Videos
                            FROM Channels
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        #st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, video_name AS Video_Title, view_count AS Views 
                            FROM Videos
                            ORDER BY view_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id AS Video_id, a.video_name AS Video_Title, comment_count
                            FROM Videos AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM Comment GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_name AS Title,like_count AS Likes_Count 
                            FROM Videos
                            ORDER BY like_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT video_name AS Title, like_count AS Likes_Count
                            FROM Videos
                            ORDER BY like_count DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, channel_views AS Views
                            FROM Channels
                            ORDER BY channel_views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name
                            FROM Videos
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name, 
                        SUM(duration_sec) / COUNT(*) AS average_duration
                        FROM (
                            SELECT channel_name, 
                            CASE
                                WHEN duration REGEXP '^PT[0-9]+H[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'H', 1), 'T', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'H', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                '0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'T', -1), ':',
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT('0:0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'T', -1)))
                                END AS duration_sec
                        FROM Videos
                        ) AS subquery
                        GROUP BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names
                          )
        st.write(df)
        st.write("### :green[Average video duration for channels :]")
        

        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comment_count AS Comments
                            FROM Videos
                            ORDER BY comment_count DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)


