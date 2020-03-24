import pandas as pd
import requests
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from config import *

import os
import googleapiclient.discovery

spotifyCredentialsManager = SpotifyClientCredentials(client_id = SPOTIFY_CLIENT_ID, client_secret = SPOTIFY_CLIENT_SECRET)
spotifyClient = spotipy.Spotify(client_credentials_manager=spotifyCredentialsManager)


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

def getCursorOfSize(collectionName, batchSize):
    count = 1
    finalResult = []
    tracks = []
    for doc in connectedDB[collectionName].find(batch_size=batchSize):
        if count % batchSize == 0:
            finalResult.append(tracks)
            tracks = []
        count+=1
        tracks.append(doc)
    finalResult.append(tracks)
    return finalResult

def getSongList():
    spotifyTracksDf = getCursorOfSize('Videos', 50)
    spotifySongs = []
    count = 1
    for row in spotifyTracksDf:
        for song in row:
            s_name = song['name']
            for artist in song['artists']:
                a_name = artist['name']
                s = s_name + ' '+ a_name
                spotifySongs.append({'_id': song['_id'], 'name': s})
    return spotifySongs

#returns videoIds, channelId, title description and other information related to the video
def getYouTubeIds():
    song_list = getSongList()
    song_list = song_list[:5]
    youtubeSongList=[]
    youtubeIdList=[]
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = YOUTUBE_API_KEY)
    for query in song_list:
        request = youtube.search().list(
            part="snippet",
            maxResults=25,
            q=query['name']
        )
        response = request.execute()
        youtubeSongList.append({'_id': query['_id'], 'youtubeId': response['items'][0]['id']['videoId']})
    return youtubeSongList


def getVideoStatistics():
    videoData =[]
    youTubeVideoList=[]
    videoIdList=getVideoIds()
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = YOUTUBE_API_KEY)
    for ids in videoIdList:
        request = youtube.videos().list(
        part="snippet,contentDetails,statistics", id=ids)
        response = request.execute()
        youTubeVideoList.append(response)
    for items in youTubeVideoList:
        for data in items['items']:
            details = {
                'id':data['id'],
                'statistics':data['statistics']}
            videoData.append(details)
    return videoData
    