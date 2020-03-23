import os
from flask import Flask
import pandas as pd
from models import *
from requester import *

app = Flask(__name__)

@app.route("/")
def main():
    spotifyTracksDf = getSpotifyTrackDetails(['3n3Ppam7vgaVa1iaRUc9Lp','3twNvmDtFQtAd5gMKedhLD'])
    result = insertManyFromDataframe('Videos', spotifyTracksDf)
    return str(result)

@app.route('/how are you')
def hello():
    return 'I am good, how about you?'

@app.route('/load_spotify_kaggle_data')
def load_spotify_kaggle_data():
    spotify_kaggle_data = load_kaggle_data()
    result = insertManyFromDataframe('Videos', spotify_kaggle_data)
    return str(result)

if __name__ == "__main__":
    app.run()