import mysql.connector
import httplib
import hashlib
import time
from datetime import datetime
from datetime import timedelta

import redis
from pymongo import MongoClient
from pymongo import IndexModel ,ASCENDING,DESCENDING
class MongoRedisUrlManager:
    def __init__(self,sever_ip='localhost',client=None,expires=timedelta(days=30)):
        self.client=MongoClient(self.SERVER_IP,27017) if client is None else client
        self.redis_client = redis.StrictRedis(host = self.SERVER_IP,port=8379,db=0)

        self.db=self.client.sprider

        if self.db.mfw.count() is 0:
            self.db.mfw.creat_index('status')

    def dequeueUrl(self):
        record= self.db.mfw.find_one_and_update({'status':'new'},
                                                {'$set':{"status":"downloading"}},
                                                {'upsert':False,'returnNewDocument':False})
        if record:
            return record
        else:
            return None

    def enqueueUrl(self ,url,status,depth):
        num=self.redis_client.get(url)
        if num is not None:
            self.redis_client.set(url,int(num)+1)
            return
        self.redis_client.set(url,1)
        record={
            'url':url,
            'status':status,
            'queue_time':datetime.utcnow(),
            'depth':depth

        }
        self.db.mfw.insert({
            'id':hashlib.md5(url).hexdigest(),
            'url':url,
            'status':status,
            'queue_time':datetime.utcnow(),
            'depth':depth
        })

    def finishUrl(self,url):
        record={'status':'done','done_time':datetime.utcnow()}
        self.db.mfw.updata({'_id':hashlib.md5(url).hexdogest()},{'set':record},upsert=False)

    def clear(self):
        self.db.mfw.drop()
