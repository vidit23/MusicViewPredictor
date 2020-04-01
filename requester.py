import pandas as pd
import requests
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import googleapiclient.discovery
from datetime import date


from config import *
import models

import os
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

spotifyCredentialsManager = SpotifyClientCredentials(client_id = SPOTIFY_CLIENT_ID, client_secret = SPOTIFY_CLIENT_SECRET)
spotifyClient = spotipy.Spotify(client_credentials_manager=spotifyCredentialsManager)
youtubeClient = googleapiclient.discovery.build('youtube', 'v3', developerKey = YOUTUBE_API_KEY)

""""getSpotifyTrackDetails takes a list of trackIds and makes two calls to the SpotifyAPI
    the first is to get the audio feautres, which includes attributes such as acousticness, danceability,
    instrumentalness, enerty etc. and the second call is to get the track details - 
    which we then filter to get only important information such as, 
    id, name, popularity, artists, album release date and album release date preceision
    The information from these two calls are stored in a dataframe and then merged. 
    The merged result is returned by the method"""
def getSpotifyTrackDetails(trackIds):
    trackFeatures = spotifyClient.audio_features(trackIds)   ##First call to get audio features
    featuresDf = pd.DataFrame(trackFeatures)                ##audio features stored as dataframe
    featuresDf.drop(['track_href', 'analysis_url', 'uri'], axis=1, inplace=True) ## audio features that are not required by us are dropped

    trackInformation = spotifyClient.tracks(trackIds) ## Second call to get the track information 
    filteredTrackInfo = []  
    ##filter the track information to extract only relevant information required for our analysis
    for track in trackInformation['tracks']:
        important_info = {'id': track['id'], 
                        'name': track['name'],
                        'popularity': track['popularity'], 
                        'artists': [{'id': artist['id'], 'name': artist['name']} for artist in track['artists']],
                        'albumReleaseDate': track['album']['release_date'], 
                        'albumReleaseDatePrecision': track['album']['release_date_precision']}
        filteredTrackInfo.append(important_info) #Only relavent information is appeneded to the list
    infoDf = pd.DataFrame(filteredTrackInfo) # filtered track information from second call is converted to a dataframe 
    ##information from the two calls (audio features and track information) which were stored as dataframes are merged here, this result is returned
    result = pd.merge(infoDf, featuresDf, how='inner', on='id', sort=True, copy=True, indicator=False, validate=None)
    result.rename({'id': '_id'}, axis=1, inplace=True) 
    return result
"""Makes a call to getCursorOfSize to get the detials of tracks in database and 
then returns the name of the song alone with artist which is used as a query to the Youtube API"""
def getYoutubeSearchQueries():
    spotifyTracksDf = models.getCursorOfSize('Videos', {'youtubeId': {'$exists': False}}, 50) #Gets those tracks from the database where there is no youtubeId
    spotifySongs = []
    for row in spotifyTracksDf:
        for song in row:
            query = song['name']
            for artist in song['artists']:
                a_name = artist['name']
                query = query + ' '+ a_name #appends the name of the song and artist 
            spotifySongs.append({'_id': song['_id'], 'name': query}) #List of dictionary with the id and the name of the song along with the artist 
    return spotifySongs


"""getYouTubeIds returns the list of youtube ids given the name of the song along with artist.
We get the name of the songs along with artist by making a call to get YoutubeSearchQueries, this result is stored in song_list
We then make a call to the Youtube API and get the list of YouTube Ids for those songs in song_list
Since there is a limit by how much we can query each day, we return an error message if the limit has been exceeded"""
def getYouTubeIds():
    """song_list is a list of dictionaries which contains the name of the song along with the artist name, 
    for eg of one of the items in the list - Freaking Me Out Ava Max the name of the query """
    song_list = getYoutubeSearchQueries()
    song_list = song_list[:50] ##get the first 50 songs in the list (Why?)
    youtubeSongList=[]
    """Each item in song_list acts as a query which we pass to the YouTube API (search) - here query is each item in song_list
    the query is a dictionary with 2 fields - id and name 
    youtubeClient helps us connect to the YouTube API""" 
    for query in song_list:
        request = youtubeClient.search().list(part="id", maxResults=1, type='video', q=query['name']) ## call to API - part gets the response by Id, maxResults tells us how many results, type we need is video and query is the name of the song along with artist
        try:
            response = request.execute()
        except:
            print('<<ERROR>> API LIMIT REACHED') ## if the youtubeAPI call limit has reached this will just return the list of youtubeSongList so far
            return youtubeSongList
        #if we get a response then we add it to the youtubeSongList with the id, and the videoID, else we print a message saying that we couldn't get a response
        if len(response['items']) != 0:
            youtubeSongList.append({'_id': query['_id'], 'youtubeId': response['items'][0]['id']['videoId']})
        else:
            print('Couldnt find search results for id= ', query['_id'])
    return youtubeSongList

"""Returns the statistics as a dictionary of dictionary """
def getVideoStatistics():
    finalDicts = []
    todayDate = date.today().strftime('%d/%m/%Y')
    #Gets the tracks from the database where there is a youtubeId and todays views are not present
    songsBatched = models.getCursorOfSize('Videos', {'youtubeId': {'$exists': True}, 'views.' + todayDate: {'$exists': False}}, 50)#List of all the songs where the row contains a youtubeId and there have been no views for today (no views for today indicates that no one has inserted today?)

    for batch in songsBatched:
        spotifyIds = []
        commaSeperatedQuery = ''
        for song in batch:
            spotifyIds.append(song['_id'])
            commaSeperatedQuery += (song['youtubeId']+',')

        commaSeperatedQuery = commaSeperatedQuery[:-1]#-1 is to indicate that the last comma is not included
        request = youtubeClient.videos().list(part='statistics', id=commaSeperatedQuery)
        #if we get a response then we add it to the youtubeSongList with the id, and the videoID, else we print a message saying that we couldn't get a response
        try:
            response = request.execute()
        except:
            print('<<ERROR>> API LIMIT REACHED')
            return finalDicts

        for index, res in enumerate(response['items']):
            finalDicts.append({'_id': spotifyIds[index], 'views.' + todayDate: res['statistics']})

    return finalDicts
    

def load_kaggle_data():
    data = pd.read_csv("/Users/nandhitharaghuram/Desktop/NYU/Spring'20/Data Science for Business Analytics/mongo_database/SpotifyFeatures.csv")
    cleaned_df = data.dropna()
    cleaned_df.rename(columns={'track_id':'_id'}, inplace=True)
    return cleaned_df