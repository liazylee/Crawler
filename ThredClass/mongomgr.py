import mysql.connector
import httplib
import hashlib
import time
from datetime import timedelta
from pymongo import MongoClient
import datetime

class MongoUrlManager:
    SERVER_IP='localhost'

    def __init__(self,client=None,expires=timedelta(days=30)):

        self.client=MongoClient(self.SERVER_IP,27017) if client is None else client
        self.db=self.client.spider

    def dequeueUrl(self):
        record=self.db.mfw.find_one_and_update(
            {'status': 'new'},
            {'$set': {'status': 'downloading'}},
            {'upsert': False, 'returnNewDocument': False}
        )
        if record:
            return record
        else:
            return  None
    def enqueueUrl(self,url,status,depth):
        record={'status':status,'queue_time':datetime.utcnow()}

        self.db.mfw.insert({'_id':url},{'$set':record})


    def finishUrl(self,url,status):
        record = {'status': status, 'done_time': datetime.utcnow()}
        self.db.mfw.update({'_id': url}, {'$set': record}, upsert=False)

    def clear(self):
        self.db.mfw.drop()

