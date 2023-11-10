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
#channel_id = "UCKZozRVHRYsYHGEyNKuhhdA" UCKZozRVHRYsYHGEyNKuhhdA
channel_id = input("Enter the channel id ")
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

def get_playlist_info(c_id):
    
    l2 = []
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId = c_id,
        maxResults=10)
    response = request.execute()
    for i in range(10):
        play_data = dict( playlist_id = response['items'][i]['id'],
                     chan_id = response['items'][i]['snippet']['channelId'],
                     play_name = response['items'][i]['snippet']['title'])
        l2.append(play_data)
    
    return l2
pl_details = get_playlist_info(channel_id)

client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['testbench_youtube']
collection = db['collection_db']

collections4 = db.playlist_details
collections4.insert_many(pl_details)

# CONNECTING WITH MYSQL DATABASE
mydb = sql.connect(host="localhost",
                   user="root",
                   password="Rohan@sql04",
                   database= "youtube_db",
                   use_pure = True
                  )
mycursor = mydb.cursor()

def insert_into_playlist():
    col4 = db[playlist_details]
    for i in col4.find({"chan_name" : user_inp},{"_id":0}):
        mycursor.execute(query1,tuple(i.values()))
        mydb.commit()

print("Loaded to Mongodb")
print("Enter the channel name to be transformed to SQL \n")
user_inp = input()

insert_into_playlist()
print("Loaded to SQL database")

