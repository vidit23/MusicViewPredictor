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
KEY_NUMBER = 0
youtubeClient = googleapiclient.discovery.build('youtube', 'v3', developerKey = YOUTUBE_API_KEY[KEY_NUMBER])

""""getSpotifyTrackDetails takes a list of trackIds and makes two calls to the SpotifyAPI
    the first is to get the audio feautres, which includes attributes such as acousticness, danceability,
    instrumentalness, enerty etc. and the second call is to get the track details - 
    which we then filter to get only important information such as, 
    id, name, popularity, artists, album release date and album release date preceision
    The information from these two calls are stored in a dataframe and then merged. 
    The merged result is returned by the method"""
def getSpotifyTrackDetails(trackIds):
    trackFeatures = spotifyClient.audio_features(trackIds)  ##First call to get audio features
    featuresDf = pd.DataFrame(trackFeatures)                ##audio features stored as dataframe
    # print(featuresDf)
    featuresDf.drop(['track_href', 'analysis_url', 'uri'], axis=1, inplace=True)  ## audio features that are not required by us are dropped

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
    spotifyTracksDf = models.getCursorOfSize('Videos', {'youtubeId': {'$exists': False}}, ['name', 'artists'], 50)  #Gets those tracks from the database where there is no youtubeId
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
    global youtubeClient
    global KEY_NUMBER
    """song_list is a list of dictionaries which contains the name of the song along with the artist name, 
    for eg of one of the items in the list - Freaking Me Out Ava Max the name of the query """
    song_list = getYoutubeSearchQueries()
    print('Got the search queries')
    song_list = song_list[:500]  ##get the first 50 songs in the list (Why?)
    youtubeSongList=[]
    """Each item in song_list acts as a query which we pass to the YouTube API (search) - here query is each item in song_list
    the query is a dictionary with 2 fields - id and name 
    youtubeClient helps us connect to the YouTube API""" 
    for query in song_list:
        request = youtubeClient.search().list(part="id", maxResults=1, type='video', q=query['name']) ## call to API - part gets the response by Id, maxResults tells us how many results, type we need is video and query is the name of the song along with artist
        try:
            response = request.execute()
        except:
            print('<<ERROR>> API LIMIT REACHED')  ## if the youtubeAPI call limit has reached this will just return the list of youtubeSongList so far
            KEY_NUMBER += 1
            if KEY_NUMBER == len(YOUTUBE_API_KEY):
                print('<<ERROR>> The last key is also over')
                return youtubeSongList  #if we get a response then we add it to the youtubeSongList with the id, and the videoID, else we print a message saying that we couldn't get a response
            try:
                youtubeClient = googleapiclient.discovery.build('youtube', 'v3', developerKey = YOUTUBE_API_KEY[KEY_NUMBER])
                continue
            except:
                print('<<ERROR>> Double error', KEY_NUMBER)
                return youtubeSongList

        if len(response['items']) != 0:
            youtubeSongList.append({'_id': query['_id'], 'youtubeId': response['items'][0]['id']['videoId']})
        else:
            print('Couldnt find search results for id= ', query['_id'])
            youtubeSongList.append({'_id': query['_id'], 'youtubeId': ''})
    return youtubeSongList

"""Returns the statistics as a dictionary of dictionary """
def getVideoStatistics():
    finalDicts = []
    global youtubeClient
    global KEY_NUMBER

    todayDate = date.today().strftime('%d/%m/%Y')
    #Gets the tracks from the database where there is a youtubeId and todays views are not present
    songsBatched = models.getCursorOfSize('Videos', {'youtubeId': {'$exists': True, '$ne': ''}, 'views.' + todayDate: {'$exists': False}}, ['youtubeId'], 50)

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
            KEY_NUMBER += 1
            if KEY_NUMBER == len(YOUTUBE_API_KEY):
                print('<<ERROR>> The last key is also over')
                return finalDicts
            try:
                youtubeClient = googleapiclient.discovery.build('youtube', 'v3', developerKey = YOUTUBE_API_KEY[KEY_NUMBER])
                continue
            except:
                print('<<ERROR>> Double error', KEY_NUMBER)
                return finalDicts

        for index, res in enumerate(response['items']):
            finalDicts.append({'_id': spotifyIds[index], 'views.' + todayDate: res['statistics']})

    return finalDicts


def getSpotifyArtistDetails(artists_ids):
    artist_details = []
    artistsInfo = spotifyClient.artists(artists_ids)['artists']
    for artistInfo in artistsInfo:
        important_info = {'_id': artistInfo['id'], 
                      'name': artistInfo['name'],
                      'popularity': artistInfo['popularity'],
                      'genres': artistInfo['genres'],
                      'followers':artistInfo['followers']['total']
                        }
        artist_details.append(important_info)
    return artist_details


def getSpotifyArtists():
    results=[]
    listOfListOfIds = models.getCursorOfSize('Videos', {}, ['artists'], 49)
    print('Read from the DB')
    artists_ids = []
    for index, idList in enumerate(listOfListOfIds):
        for track in idList: #each track is just one row in the database
            for artist_id in track['artists']:
                artists_ids.append(artist_id['id'])
    artists_ids_set = set(artists_ids) 
    unique_artist_list = (list(artists_ids_set))
    index = 0
    while index < len(unique_artist_list):
        spotifyArtistDf = getSpotifyArtistDetails(unique_artist_list[index:index+50])
        spotifyArtistDf = pd.DataFrame(spotifyArtistDf)
        results.append(spotifyArtistDf)
        index += 50
        # result = insertManyFromDataframe('Artists', spotifyArtistDf)
    return results
