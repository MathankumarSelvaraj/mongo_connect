
import pymongo as pym
from bson import ObjectId
from pymongo import MongoClient
from datetime import datetime, tzinfo, timezone
import pandas as pd
from datetime import datetime
import isodate
import os
import json
from bson.json_util import dumps
from config import uri

uri = os.getenv("uri")
cluster=MongoClient(uri) 

print(uri)

dbs = cluster.list_database_names()
Cezdb= cluster["cargoez"]
cez_report = Cezdb["reports"]


trucker=   [
  {'$match':{
                'companyId': ObjectId("5bd008758adc0c324825699c"),
                'reportSettingsId': ObjectId( "620ba15c382f85c7c5e0125f"),
                "createdAt": { '$gte': datetime(2020, 12, 31, 8, 0, 0, tzinfo=timezone.utc), 
                '$lt': datetime(2023, 1, 1, 8, 0, 0, tzinfo=timezone.utc)}
             },
  },
  {
    '$addFields':{
        'trucker': {'$arrayToObject': "$parties"},
        'comm': {'$map': 
                        { 'input': "$commodities",
                            'as': "weight",
                            'in': {'$sum': "$$weight.grossWeight"}
                         }
                 }
                },
  },

  {
    '$group':
      {
        '_id': {
          'truk': "$trucker.localDelivery.name",
          'o_port': "$shipment.originPort.code",
          'd_port': "$shipment.destinationPort.code",
          'month' : {'$month':"$createdAt"},
          'year' :{'$year':"$createdAt"}
        },
        'count': {'$sum': 1,},
      'weight': {
          '$sum': {
            '$first': "$comm",
          },
        },
      },
  },
  {
   '$sort':
     
      {
        'count': -1,
      },
  },
]


result=cez_report.aggregate(trucker)


list_cur = list(result)

json_data=dumps(list_cur)

df = pd.json_normalize(json.loads(json_data))


df.head(5)
#df.to_csv(r'C:\Users\dataq\OneDrive\Desktop\trucker_data.csv')

