from pymongo import mongo_client
from src.config import config
from motor.motor_asyncio import AsyncIOMotorClient
import json

client = AsyncIOMotorClient(config['DATABASE_URL'])
db = client[config['MONGO_INITDB_DATABASE']]
print('Connected to MongoDB')


db = client.get_database(config['MONGO_INITDB_DATABASE'])

Embedded_movies = db.embedded_movies
Movies = db.movies
Sessions = db.sessions
Movies2 = db.movies2
Embedded_movies_new = db.embedded_movies_new
Embedded_movies2 = db.embedded_movies2


