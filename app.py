import os
from flask import Flask
import pandas as pd
from models import *
from requester import *
import time

app = Flask(__name__)

@app.route("/spotifyTracks")
def main():
    listOfListOfIds = getCursorOfSize('Videos', {}, ['_id'], 49)
    print('Read from the DB')
    for index, idList in enumerate(listOfListOfIds):
        # print(idList)
        spotifyTracksDf = getSpotifyTrackDetails([id_dict['_id'] for id_dict in idList])
        result = updateManyFromDataframe('Videos', spotifyTracksDf)
        print(index)
        time.sleep(1)
        # break
    return str(result)

@app.route('/youtubeIds')
def getCorrespondingYoutubeIds():
    correspondingIds = getYouTubeIds()
    print('Got the ids, writing to DB')
    result = updateManyDocument('Videos', correspondingIds)
    print('Finished the youtubeIds API')
    return str(result)

@app.route('/youtubeStats')
def getYoutubeStats():
    idsAndStats = getVideoStatistics()
    print('Got the stats, writing to DB')
    index = 0
    result = []
    while index < len(idsAndStats):
        result = updateManyDocument('Videos', idsAndStats[index:index+400])
        index += 400
    print('Finished the youtubeStats API')
    return str(result)

@app.route("/spotifyArtists")
def getSpotifyArtistsAPI():
    artists = getSpotifyArtists()
    for df in artists:
        result = insertManyFromDataframe('Artists', df)
    return str(result)



if __name__ == "__main__":
    app.run()

# mongoimport --host MVP-shard-0/mvp-shard-00-00-bvqf2.mongodb.net:27017,mvp-shard-00-01-bvqf2.mongodb.net:27017,mvp-shard-00-02-bvqf2.mongodb.net:27017 --ssl --username vidit23 --password dsba123 --authenticationDatabase admin --db MVP --collection Videos --type CSV --file ./data/SpotifyFeatures.csv --fields genre,artist_name,name,_id,popularity,acousticness,danceability,duration_ms,energy,instrumentalness,key,liveness,loudness,mode,speechiness,tempo,time_signature,valence