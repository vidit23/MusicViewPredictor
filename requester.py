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
    spotifyTracksDf=getSpotifyTrackDetails(trackIds)## to be replaced by fetching information from mongodb
    spotifyTracksdict = spotifyTracksDf.to_dict("records")
    spotifySongs = []
    for row in spotifyTracksdict:
        song=row['name']
        #append the song for each artist in this row
        for artist in row['artists']:
        spotifySongs.append(artist['name'] + song)
    return spotifySongs

def getYouTubeIds(trackIds):
    song_list = getSongList(trackIds)
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
            q=query
        )
        youtubeSongList.append(response)
    for songs in youtubeSongList:
        youtubeIdList.append(songs['items'][0])
    return youtubeIdList