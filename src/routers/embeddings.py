from fastapi import APIRouter, Depends, HTTPException, Request, status, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import bcrypt,json
from src import schemas

from src.config import config
router = APIRouter()
from bson.objectid import ObjectId
from src.utils.ada_embedder import embed_movie as embed_movie_ada, get_embedding as get_embedding_ada
from src.db import Movies, Embedded_movies,db
from src.cache_system import r
import operator

vs_penalty=1
fts_penalty=1
vs_const=1
fts_const=1

@router.get("/init_embeddings")
async def init_embeddings():
    try:
        movies = await Movies.find().to_list(25000)
        # return {"message": "Embeddings initialized"}
        for movie in movies:
            # print(f"Generating embedding for {movie['title']}")
            if "plot" not in movie:
                continue
            embedding = get_embedding_ada([movie['plot']])
            embedding = embedding[0].tolist()[0]
            embedding = [float(value) for value in embedding]
            movie['embedding'] = embedding
            await Embedded_movies.insert_one(movie)
            if embedding is None:
                print(f"Failed to embed {movie['title']}")
            else :
                # print(f"Success")
                pass
        
        return {"message": "Embeddings initialized"}
    except Exception as e:
        raise HTTPException(status_code = 500, detail=str(e))
def get_score(element):
    return element['score'],element['imdb']['rating']

@router.post("/fts_search")
async def fts_search(request: schemas.RRFQuerySchema):
    query = request.query
    key=query+'@ftssearch'
    value = r.get(key)
    if value:
        return json.loads(value)
    
    pipeline2=[
        {
        '$search': {
            'index': "movie_index",
            "compound": {
                "should": [
                    {
                    "text": {
                        "query": query, 
                        "path": 'title',
                        "fuzzy":{'prefixLength':3,'maxExpansions':500},
                        "score":{
                        "boost":{
                            "value":5
                        }
                        }
                    }
                    }, {
                            "text": {
                                "query": query, 
                                "path": 'genres',
                                "fuzzy":{'prefixLength':1,},
                                "score":{
                                "boost":{
                                    "value":3
                                }
                                }
                            }
                        }, {
                            "text": {
                                "query": query, 
                                "path": 'cast',
                                "fuzzy":{'prefixLength':1},
                                "score":{
                                "boost":{
                                    "value":2
                                }
                                }
                            }
                        }, {
                            "text": {
                                "query": query, 
                                "path": 'directors',
                                "score":{
                                "boost":{
                                    "value":1
                                }
                                }
                            }
                        }
                ], 
                "minimumShouldMatch": 1
            },
            "highlight":{
                "path":{
                    "wildcard":"*"
                }
            }
        }},
        { '$limit': 100 },
        { '$project': { '_id': 1, 'title': 1, 'imdb': 1, 'plot': 1, 'poster_path': 1, 'runtime': 1, 'year': 1,"highlights":{"$meta":"searchHighlights"},"score":{"$meta":"searchScore"} }}
    ]
    
    results =await Movies.aggregate(pipeline2).to_list(100)
    # print(results)
    for i in range(len(results)):
        results[i]["_id"] = str(results[i]["_id"])
        results[i]["title"] = str(results[i]["title"])
        results[i]['score']=1/(i+fts_const+fts_penalty)
    r.set(key,json.dumps(results))
    return results

@router.post("/sem_search")
async def sem_search(request: schemas.RRFQuerySchema):
    query = request.query
    query_vector = get_embedding_ada([query])  
    query_vector = query_vector[0].tolist()[0]
    query_vector_bson = [float(value) for value in query_vector]
    # print(query,query_vector_bson)
    key=query+'@sem'
    value = r.get(key)
    # print(value)
    if value:
        return json.loads(value)
    pipeline = [
    {
        '$vectorSearch': {
        'index': 'vector_index', 
            'path': 'embedding',  
            'queryVector': query_vector_bson,
            'numCandidates': 200, 
        'limit': 100
        }
    },
    {
        '$project': {
            '_id': 1,
            'title':1,
            # 'vs_score':1,
            'imdb': 1, 'plot': 1, 'poster_path': 1, 'runtime': 1, 'year': 1,
            'score':{"$meta":"vectorSearchScore"}
        }
    }
    ]

    results = await Embedded_movies.aggregate(pipeline).to_list(100)
    for i in range(len(results)):
        results[i]["_id"] = str(results[i]["_id"])
        results[i]["title"] = str(results[i]["title"])
        results[i]['score']=1/(i+vs_const+vs_penalty)
    r.set(key,json.dumps(results))
    return results


@router.post("/rrf")
async def rrf(request: schemas.RRFQuerySchema):
    query = request
    query = query.query
    key=query+'@rrf'
    value = r.get(key)
    print(f"Testing for Cache Key: {key}")
    if value:
        print("Cache Hit")
        return json.loads(value)
    resultVs=[]
    resultFts=[]
    resultVs = await sem_search(schemas.RRFQuerySchema(query=query))
    # print(resultVs)
    if len(query) < 50:
        resultFts=await fts_search(schemas.RRFQuerySchema(query=query))
    # print(resultFts)
    response = []
    
    for result in resultFts:
        response.append(result)
        
    for result in resultVs:
        _id=result['_id']
        for checker in response:
            if checker['_id'] == _id:
                checker['score'] += result['score']
                result['_id']=""
        if result['_id'] != "":
            db_movie=await Movies.find_one({'_id':ObjectId(_id)})
            if db_movie and ('poster_path' in db_movie) and db_movie['poster_path']:
                result['poster_path']=db_movie['poster_path']
            response.append(result)    
            
    response.sort(reverse=True,key=lambda elem: "%s %s" % (elem['score'], elem['imdb']['rating']))  
    response=response[0:50]
    r.set(key,json.dumps(response)) 
    print(f"Cache Miss: Generated Key {key}")
    return response[0:50]


@router.post('/fts_search_filter')
async def fts_search_filter(request: schemas.FilterSchema):
    query=request.query
    genres=request.genre
    year=request.year
    language=request.language
    
    key=query+'_'+(str(genres) or 'genre_None')+'_'+(str(year) or 'year_None')+'_'+(language or 'language_None')+'@rrf'
    value = r.get(key)
    if value:
        return json.loads(value)
    pipeline2=[
        {
        '$search': {
            'index': "movie_index",
            "compound": {
                "must":[],
                "should": [
                    {
                    "text": {
                        "query": query, 
                        "path": 'title',
                        "fuzzy":{'prefixLength':1,'maxExpansions':70},
                        "score":{
                        "boost":{
                            "value":5
                        }
                        }
                    }
                    },{
                            "text": {
                                "query": query, 
                                "path": 'cast',
                                "fuzzy":{'prefixLength':1,},
                                "score":{
                                "boost":{
                                    "value":3
                                }
                                }
                            }
                        }, {
                            "text": {
                                "query": query, 
                                "path": 'directors',
                                "score":{
                                "boost":{
                                    "value":2
                                }
                                }
                            }
                        }
                ], 
                "minimumShouldMatch": 1
            },
            "highlight":{
                "path":{
                    "wildcard":"*"
                }
            }
        }},
        { '$limit': 100 },
        { '$project': { '_id': 1, 'title': 1, 'imdb': 1, 'plot': 1, 'poster_path': 1, 'runtime': 1, 'year': 1,"highlights":{"$meta":"searchHighlights"},"score":{"$meta":"searchScore"} }},
        { '$sort': { 'score': -1,'imdb': -1} }
    ]
    for genre in genres:
        if genre:
            pipeline2[0]['$search']['compound']['must'].append({"text": {"query": genre,"path": "genres"}})
    if year:
        pipeline2[0]['$search']['compound']['must'].append({'range': {'path': 'year', 'gte': year, 'lte': year}})
    if language:
        pipeline2[0]['$search']['compound']['must'].append({"text": {"query": language,"path": "languages"}})
        
    results =await Movies.aggregate(pipeline2).to_list(100)
    # print(results)
    for i in range(len(results)):
        results[i]["_id"] = str(results[i]["_id"])
        results[i]["title"] = str(results[i]["title"])
    r.set(key,json.dumps(results))
    return results
