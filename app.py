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


if __name__ == "__main__":
    app.run()