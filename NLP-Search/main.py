from nl2query import MongoQuery
import pymongo
import dotenv
import os

dotenv.load_dotenv()

client = pymongo.MongoClient(os.getenv('DATABASE_URL'))

print("Connected to Mongo")

db = client.get_database(os.getenv('MONGO_INITDB_DATABASE'))


keys = list(db.movies.find_one().keys())
print(keys)
print(type(keys))
keys.append('index')
queryfier = MongoQuery(keys, 'movies')
query = queryfier.generate_query('''Action movies released after 2010 with a rating greater than 8.5 and directed by Christopher Nolan.''')

res = eval(query)

if res != None:
    for i in res:
        print(i)


print(query)
