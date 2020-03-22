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

def getSongList(trackIds):
    spotifyTracksDf=getSpotifyTrackDetails(trackIds)
    spotifyIdArtists = spotifyTracksDf[['artists','_id']]
    spotifyArtists = spotifyTracksDf['artists'].values.tolist()
    flat_list = []
    for sublist in spotifyArtists:
        for item in sublist:
            flat_list.append(item)
    spotifyArtists=pd.DataFrame(flat_list)
    spotifyArtists.columns=['artist_id','artist_name']
    spotifyMergedDf=pd.concat([spotifyTracksDf,spotifyArtists], axis=1)
    song_list = []
    for index,row in spotifyMergedDf.iterrows():
        song = row['artist_name'] +" "+row['name']
        song_list.append(song)
    return song_list

def getYouTubeIds(trackIds):
    song_list = getSongList(trackIds)
    youtubeSongList=[]
    youtubeIdList=[]
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = YOUTUBE_API_KEY
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)
    for query in song_list:
        request = youtube.search().list(
            part="snippet",
            maxResults=25,
            q=query
        )
        youtubeSongList.append(response)
    for songs in youtubeSongList:
        youtubeIdList.append(songs['items'][0])
    return youtubeIdList