import os
from flask import Flask
import pandas as pd
from models import *
from requester import *

app = Flask(__name__)

@app.route("/spotifyTracks")
def main():
    spotifyTracksDf = getSpotifyTrackDetails(['3n3Ppam7vgaVa1iaRUc9Lp','3twNvmDtFQtAd5gMKedhLD'])
    result = insertManyFromDataframe('Videos', spotifyTracksDf)
    return str(result)

@app.route('/youtubeIds')
def getCorrespondingYoutubeIds():
    correspondingIds = getYouTubeIds()
    for dicts in correspondingIds:
        updateOneDocument('Videos', dicts['_id'], dicts)
    return str(correspondingIds)

@app.route('/youtubeStats')
def getYoutubeStats():
    idsAndStats = getVideoStatistics()
    for dicts in idsAndStats:
        updateOneDocument('Videos', dicts['_id'], dicts)
    return str(idsAndStats)

@app.route('/load_spotify_kaggle_data')
def load_spotify_kaggle_data():
    spotify_kaggle_data = load_kaggle_data()
    i = 0
    for i in range(len(spotify_kaggle_data)):
        result = insertManyFromDataframe('Videos', spotify_kaggle_data[i:i+100])
        i = i+100
    return str(result)


if __name__ == "__main__":
    app.run()