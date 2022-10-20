import pymongo
import csv,json

connection_string = "mongodb+srv://<username>:<password>@cluster0.6x0bs79.mongodb.net/test"

client  = pymongo.MongoClient(connection_string)

def getDataFromCSV():
    header = [ "userid", "age", "dob_day", "dob_year", "dob_month", "gender", "tenure", "friend_count", "friendships_initiated", "likes", "likes_received", "mobile_likes", "mobile_likes_received", "www_likes", "www_likes_received" ]
    csvfile = open('pseudo_facebook.csv', 'r')
    reader = csv.DictReader(csvfile)
    data = []
    for each in reader:
        row={}
        for field in header:
            row[field]=each[field]
        data.append(row)
    return data

db = client['Facebook-Analysis']
collection = db.analysis
analysis = getDataFromCSV()
result = collection.insert_many(analysis)


documents = db.analysis.aggregate([
    #where age == 32
    {"$match": {"age": "32"}},
    #count of males & females of age 32
    {"$group": {"_id": "$gender", "count": {"$sum": 1}}},
    #get total likes of users of age 32
    {"$group": {"_id": "$_id","Totallikes": {"$sum": {"$add": [{"$toInt": "$likes"}, {"$toInt":"$likes_received"}]}}}},
    #sort ascending order of likes
    {"$sort": {"Totallikes": 1}},
])

print(documents)

with open("data.json","a") as f:
    for document in documents:
        f.write(str(document))

