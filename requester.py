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


def getSpotifyTrackDetails(trackIds):
    trackFeatures = spotifyClient.audio_features(trackIds)
    featuresDf = pd.DataFrame(trackFeatures)
    # print(featuresDf)
    featuresDf.drop(['track_href', 'analysis_url', 'uri'], axis=1, inplace=True)

    trackInformation = spotifyClient.tracks(trackIds)
    filteredTrackInfo = []
    for track in trackInformation['tracks']:
        important_info = {'id': track['id'], 
                        'name': track['name'],
                        'popularity': track['popularity'], 
                        'artists': [{'id': artist['id'], 'name': artist['name']} for artist in track['artists']],
                        'albumReleaseDate': track['album']['release_date'], 
                        'albumReleaseDatePrecision': track['album']['release_date_precision']}
        filteredTrackInfo.append(important_info)
    infoDf = pd.DataFrame(filteredTrackInfo)

    result = pd.merge(infoDf, featuresDf, how='inner', on='id', sort=True, copy=True, indicator=False, validate=None)
    result.rename({'id': '_id'}, axis=1, inplace=True)
    return result


def getYoutubeSearchQueries():
    spotifyTracksDf = models.getCursorOfSize('Videos', {'youtubeId': {'$exists': False}}, ['name', 'artists'], 50)
    spotifySongs = []
    for row in spotifyTracksDf:
        for song in row:
            query = song['name']
            for artist in song['artists']:
                a_name = artist['name']
                query = query + ' '+ a_name
            spotifySongs.append({'_id': song['_id'], 'name': query})
    return spotifySongs


#returns videoIds, channelId, title description and other information related to the video
def getYouTubeIds():
    global youtubeClient
    global KEY_NUMBER
    song_list = getYoutubeSearchQueries()
    print('Got the search queries')
    song_list = song_list[:500]
    youtubeSongList=[]
    for query in song_list:
        request = youtubeClient.search().list(part="id", maxResults=1, type='video', q=query['name'])
        try:
            response = request.execute()
        except:
            print('<<ERROR>> API LIMIT REACHED')
            KEY_NUMBER += 1
            if KEY_NUMBER == len(YOUTUBE_API_KEY):
                print('<<ERROR>> The last key is also over')
                return youtubeSongList
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


def getVideoStatistics():
    finalDicts = []
    global youtubeClient
    global KEY_NUMBER

    todayDate = date.today().strftime('%d/%m/%Y')
    songsBatched = models.getCursorOfSize('Videos', {'youtubeId': {'$exists': True, '$ne': ''}, 'views.' + todayDate: {'$exists': False}}, ['youtubeId'], 50)

    for batch in songsBatched:
        spotifyIds = []
        commaSeperatedQuery = ''
        for song in batch:
            spotifyIds.append(song['_id'])
            commaSeperatedQuery += (song['youtubeId']+',')

        commaSeperatedQuery = commaSeperatedQuery[:-1]
        request = youtubeClient.videos().list(part='statistics', id=commaSeperatedQuery)
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


def load_kaggle_data():
    data = pd.read_csv("./data/SpotifyFeatures.csv")
    cleaned_df = data.dropna()
    cleaned_df.rename(columns={'track_id':'_id'}, inplace=True)
    return cleaned_df