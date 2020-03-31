import pymongo
import pandas as pd
from config import *

print("Connecting to the DB")
client = pymongo.MongoClient("mongodb+srv://" + MONGO_ATLAS_USER + ":" + MONGO_ATLAS_PASSWORD + "@mvp-bvqf2.mongodb.net/test?retryWrites=true&w=majority")
connectedDB = client['MVP']


def insertManyFromDataframe(collectionName, data):
    data_dict = data.to_dict("records")
    try:
        inserted = connectedDB[collectionName].insert_many(data_dict)
        return inserted.inserted_ids
    except Exception as err:
        print('Could not insert into the collection ', collectionName)
        print('Because of ', err)
        return []

def getCursorOfSize(collectionName, query, batchSize):
    count = 1
    finalResult = []
    tracks = []
    for doc in connectedDB[collectionName].find(query, batch_size=batchSize):
        if count % batchSize == 0:
            finalResult.append(tracks)
            tracks = []
        count+=1
        tracks.append(doc)
    finalResult.append(tracks)
    return finalResult

def updateManyFromDataframe(collectionName, data):
    data_dict = data.to_dict("records")
    for data in data_dict:
        try:
            connectedDB[collectionName].replace_one({'_id': data['_id']}, data)
        except Exception as err:
            print('Could not insert into the collection ', collectionName)
            print('Because of ', err)
            return []

def updateOneDocument(collectionName, documentId, updateQuery):
    return connectedDB[collectionName].update({'_id': documentId}, {'$set': updateQuery})
