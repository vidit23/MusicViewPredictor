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


