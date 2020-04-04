import pymongo
from pymongo import InsertOne, DeleteOne, ReplaceOne, UpdateOne
import pandas as pd
from config import *

print("Connecting to the DB")
# client = pymongo.MongoClient("mongodb+srv://" + MONGO_ATLAS_USER + ":" + MONGO_ATLAS_PASSWORD + "@mvp-bvqf2.mongodb.net/test?retryWrites=true&w=majority")
client = pymongo.MongoClient("mongodb://localhost:27017/MVP")
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


def updateManyFromDataframe(collectionName, data):
    data_dict = data.to_dict("records")
    bulkOps = []
    for data in data_dict:
        bulkOps.append(UpdateOne({'_id': data['_id']}, {'$set': data}))
    try:
        inserted = connectedDB[collectionName].bulk_write(bulkOps)
        return inserted.bulk_api_result
    except Exception as err:
        print('Could not insert into the collection ', collectionName)
        print('Because of ', err)
        return []


def getCursorOfSize(collectionName, query, projection, batchSize):
    count = 0
    finalResult = []
    tracks = []
    for doc in connectedDB[collectionName].find(query, projection=projection, batch_size=batchSize):
        count+=1
        tracks.append(doc)
        if count % batchSize == 0:
            finalResult.append(tracks)
            tracks = []
        
    finalResult.append(tracks)
    return finalResult


# def updateManyFromDataframe(collectionName, data):
#     data_dict = data.to_dict("records")
#     for data in data_dict:
#         try:
#             connectedDB[collectionName].update({'_id': data['_id']}, {'$set': data})
#         except Exception as err:
#             print('Could not insert into the collection ', collectionName)
#             print('Because of ', err)
#             return []


def updateOneDocument(collectionName, documentId, updateQuery):
    return connectedDB[collectionName].update({'_id': documentId}, {'$set': updateQuery})

# def upsertManyDocument(data):
#     data_dict = data.to_dict("records")
#     return UpdateOne(data_dict,upsert=True)