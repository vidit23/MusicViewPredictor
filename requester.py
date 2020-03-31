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


def getSpotifyTrackDetails(trackIds):
    trackFeatures = spotifyClient.audio_features(trackIds)
    featuresDf = pd.DataFrame(trackFeatures)
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
    spotifyTracksDf = models.getCursorOfSize('Videos', {'youtubeId': {'$exists': False}}, 50)
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
    song_list = getYoutubeSearchQueries()
    song_list = song_list[:50]
    youtubeSongList=[]
    for query in song_list:
        request = youtubeClient.search().list(part="id", maxResults=1, type='video', q=query['name'])
        try:
            response = request.execute()
        except:
            print('<<ERROR>> API LIMIT REACHED')
            return youtubeSongList
        if len(response['items']) != 0:
            youtubeSongList.append({'_id': query['_id'], 'youtubeId': response['items'][0]['id']['videoId']})
        else:
            print('Couldnt find search results for id= ', query['_id'])
    return youtubeSongList


def getVideoStatistics():
    finalDicts = []
    todayDate = date.today().strftime('%d/%m/%Y')
    songsBatched = models.getCursorOfSize('Videos', {'youtubeId': {'$exists': True}, 'views.' + todayDate: {'$exists': False}}, 50)

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
            return finalDicts

        for index, res in enumerate(response['items']):
            finalDicts.append({'_id': spotifyIds[index], 'views.' + todayDate: res['statistics']})

    return finalDicts
    