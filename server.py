from flask_sock import Sock
from bson import UUID_SUBTYPE, Binary
from bson.json_util import dumps,loads
from flask import Flask,request, json,jsonify
from flask_pymongo import PyMongo
from uuid import UUID
from bson.objectid import ObjectId
import redis


app = Flask(__name__)
sock = Sock(app)
app.config['MONGO_URI'] = 'mongodb+srv://shetesaurabh872:saurabh5353@cluster0.xu1axex.mongodb.net/decision_tree_framework?retryWrites=true&w=majority'
mongo = PyMongo(app)
r = redis.Redis(
  host='redis-14966.c264.ap-south-1-1.ec2.cloud.redislabs.com',
  port=14966,
  password='MF5nuzX1ldHEMABcAPXCWMBDhtKdbIEx')
def root_nodes(json_data):
        if r.exists(':'.join(json_data['keywords'])):
            result=r.get(':'.join(json_data['keywords']))
            return result

        data=[]
        pipeline = [
        {
            "$addFields": {
                "commonWordsCount": {
                    "$size": {
                        "$setIntersection": ["$keywords", json_data['keywords']]
                    }
                }
            }
        },
        {
            "$match": {
                "commonWordsCount": {"$gt": 0}
            }
        },
        {
            "$sort": {
                "commonWordsCount": -1  
            }
        }
    ]
        result = mongo.db.root_nodes.aggregate(pipeline)
        formatted_data = [
            {**doc, '_id': str(doc['_id'])} for doc in result
        ]
        data.extend(formatted_data)
        if not data:
            return dumps({"message": "No root nodes found"})
        else: 
            r.set(':'.join(json_data['keywords']), dumps(data))
            return dumps(data)


@sock.route('/get-data')
def child_nodes(ws):
    while True:
        try: 
            message=ws.receive()
            json_data = loads(message)
            if(json_data["flag"]==True):
                result=root_nodes(json_data)
                ws.send((result))
            else:
                object_id = ObjectId(json_data["id"])
                data = mongo.db.child_nodes.find_one({"_id":object_id})
                if data is None:
                    ws.send(dumps({"message": "Records not found"}))
                else:
                    ws.send(dumps(data))
        except Exception as e:
            ws.send(dumps({"message": "Error occured"}))

if __name__ == "__main__":
  sock.run(app, debug=True)