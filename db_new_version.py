from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import os

def read_db():
    # %% connect to SCDx
    #client = MongoClient(
    #    "mongodb://pmocref:PmoCR3f@ims-mngdb-rtp-d-06:37600/admin?connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-1")

    # somehow when this run on the openstack it shows timeout
    client = MongoClient(
        "mongodb://pmocref:PmoCR3f@ims-mngdb-rtp-d-06:37600/")
    database = client["pmocscdb"]
    collection = database["commonVersion"]
    org='FOL'
    result=collection.find({'planningOrg':org},{'_id':0,'planningOrg':1,'version':1}).limit(100)
    for x in result:
        print(x)

if __name__=='__main__':
    read_db()