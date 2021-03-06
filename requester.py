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
    song_list = song_list[:400]
    youtubeSongList=[]
    for query in song_list:
        request = youtubeClient.search().list(part="id", maxResults=1, type='video', q=query['name'])
        try:
            response = request.execute()
        except:
            print('<<ERROR>> API LIMIT REACHED: used number', KEY_NUMBER,'out of', len(YOUTUBE_API_KEY))
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


def getSpotifyPopularity(trackIds):
    global spotifyClient
    trackInformation = spotifyClient.tracks(trackIds)
    trackPopularity = {}
    for track in trackInformation['tracks']:
        trackPopularity[track['id']] = track['popularity']
    return trackPopularity


def getVideoStatistics():
    finalDicts = []
    global youtubeClient
    global KEY_NUMBER

    todayDate = date.today().strftime('%d/%m/%Y')
    songsBatched = models.getCursorOfSize('Videos', {'youtubeId': {'$exists': True, '$ne': ''}, 'views.' + todayDate: {'$exists': False}}, ['youtubeId'], 50)
    print('Fetched', len(songsBatched), 'batches from the DB')
    for batch in songsBatched:
        spotifyIds = []
        commaSeperatedQuery = ''
        youtubeSpotifyIdMapping = {}
        for song in batch:
            spotifyIds.append(song['_id'])
            commaSeperatedQuery += (song['youtubeId']+',')
            youtubeSpotifyIdMapping[song['youtubeId']] = song['_id']

        commaSeperatedQuery = commaSeperatedQuery[:-1]
        request = youtubeClient.videos().list(part='statistics', id=commaSeperatedQuery)
        try:
            response = request.execute()
        except:
            print('<<ERROR>> API LIMIT REACHED', KEY_NUMBER,'out of', len(YOUTUBE_API_KEY))
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

        spotifyPopularityDict = getSpotifyPopularity(youtubeSpotifyIdMapping.values())
        for res in response['items']:
            try:
                tempDict = {'_id': youtubeSpotifyIdMapping[res['id']], 'views.' + todayDate: res['statistics']}
                tempDict['views.' + todayDate]['spotifyPopularity'] = spotifyPopularityDict[youtubeSpotifyIdMapping[res['id']]]
                finalDicts.append(tempDict)
            except:
                print('Key didnt exist', res)
                continue

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