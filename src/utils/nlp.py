from nl2query import MongoQuery
import pymongo
import dotenv
import os
import re
import json
import sys
from src.db import word2vec_model, db
print("Loaded")
def compute_similarity(word1, word2):
    if word1 in word2vec_model.key_to_index and word2 in word2vec_model.key_to_index:
        similarity_score = word2vec_model.similarity(word1, word2)
        return similarity_score
    else:
        return None
def similaritybwwords(s1, s2):
    set1 = set(s1)
    set2 = set(s2)
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union
def get_word2vec_model(str1, keys):
    quoted_strings = re.findall(r'"([^"]*)"', str1)
    for stri in quoted_strings:
        if stri[0] == '$':
            continue
        print("current string",stri)
        similarity_score = 0
        similarity_string =""
        for key in keys:
            got_score = compute_similarity(stri , key)
            if got_score is not None:
                if similarity_score < got_score:
                    similarity_score = got_score
                    similarity_string = key
        print("Similarity Score : ",similarity_score , "Similarity Word : ",similarity_string)
        if similarity_string != "":
            str1.replace(f'"{stri}"' , f'"{similarity_string}"')
    return str1
# dotenv.load_dotenv()
# client = pymongo.MongoClient(os.getenv('DATABASE_URL'))
# print("Connected to Mongo")
# db = client.get_database(os.getenv('MONGO_INITDB_DATABASE'))
async def nlp_processing(passed_query):
    try:
        keys = ['index' , 'title', 'genres', 'year', 'imdb.rating' ,'cast']
        queryfier = MongoQuery(keys, 'movies')
        query = queryfier.generate_query(passed_query)
        if "set" in query:
            print("Cannot Set")
            return ""
        # print(query)
        print("Run Query")
        quoted_strings = re.findall(r'"([^"]*)"', query)
        for i in quoted_strings:
            closest = 0.0
            keyval = ""
            # matching similarity with keys
            for key in keys:
                similarity = similaritybwwords(i, key)
                if similarity > closest:
                    closest = similarity
                    keyval = key
            if(closest > 0.8):
                query = query.replace(f'"{i}"', f'"{keyval}"')
        query = query.replace(f'Movies', f'movies')
        print(query)
        query = get_word2vec_model(query , keys)
        query = re.sub(r'\btrue\b', '"True"', query)
        query = re.sub(r'\bfalse\b', '"False"', query)
    except:
        return ""
    try:
        while query[0] != '{':
            query=query[1:]
        for i in range(len(query)-1,-1,-1):
            if query[i] == '}':
                break
            else:
                query=query[:-1]
        
        if '_id' in query:
            query=query[:-1]
            for i in range(len(query)-1,-1,-1):
                if query[i] == '}':
                    break
                else:
                    query=query[:-1]
    except:
        return ""
    print(query)
    return query