from fastapi import APIRouter, Depends, HTTPException, Request, status, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import bcrypt
from src import schemas

from src.config import config
router = APIRouter()
from bson.objectid import ObjectId
from src.utils.ada_embedder import embed_movie as embed_movie_ada, get_embedding as get_embedding_ada
from src.db import Movies, Embedded_movies
from src.cache_system import r



@router.get("/init_embeddings")
async def init_embeddings():
    try:
        movies = await Movies.find().to_list(25000)
        # return {"message": "Embeddings initialized"}
        for movie in movies:
            print(f"Generating embedding for {movie['title']}")
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
                print(f"Success")
        
        return {"message": "Embeddings initialized"}
    except Exception as e:
        raise HTTPException(status_code = 500, detail=str(e))
def get_score(element):
    return element['score']

@router.post("/fts_search")
async def fts_search(request: schemas.FTSQuerySchema):
    query = request.query
    arg=request.arg
    pipeline=[
        {
        '$search': {
            'index': "movie_index",
            'text': {
                'query': query,
                'path': arg,
                'fuzzy':{},
                'score': {
                    'boost': {
                    'value': 5
                    }
                }
            },
            "highlight":{
                "path":arg
            }
        }
        },
        { '$limit': 100 },
        { '$project': { '_id': 1, 'title': 1, 'imdb': 1, 'plot': 1, 'poster_path': 1, 'runtime': 1, 'year': 1,"highlights":{"$meta":"searchHighlights"} ,"score":{"$meta":"searchScore"}}}  
    ]
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
                        "fuzzy":{},
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
                                "fuzzy":{},
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
                                "fuzzy":{},
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
                                "fuzzy":{},
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
    
    if arg == '*':
        finalPipeline=pipeline2
    else:
        finalPipeline=pipeline
    results = await Movies.aggregate(finalPipeline).to_list(100)
    # print(results)
    maxScore=results[0]['score']
    for result in results:
        result["_id"] = str(result["_id"])
        result["title"] = str(result["title"])
        result['score']/=maxScore
    # r.set(key,json.dumps(response))
    return results

@router.post("/sem_search")
async def sem_search(request: schemas.RRFQuerySchema):
    # print("Harsh 6666")
    query = request.query
    query_vector = get_embedding_ada([query])  
    query_vector = query_vector[0].tolist()[0]
    query_vector_bson = [float(value) for value in query_vector]
    print(query,query_vector_bson)
    # key=query+'@sem'
    # value = r.get(key)
    # print(value)
    # if value:
    #     return json.loads(value)
    vector_penalty = 3
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
    # {
    #     "$group": {
    #         "_id": None,
    #         "docs": {"$push": "$$ROOT"}
    #     }
    # }, 
    # {
    #     "$unwind": {
    #         "path": "$docs", 
    #         "includeArrayIndex": "rank"
    #     }
    # },
    # {
    #     "$addFields": {
    #         "vs_score": {
    #             "$divide": [1.0, {"$add": ["$rank",vector_penalty , 1]}]
    #         }
    #     }
    # }, 
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

    results = await Embedded_movies.aggregate(pipeline).to_list(10)
    for result in results:
        result["_id"] = str(result["_id"])
        result["title"] = str(result["title"])
    # r.set(key,json.dumps(response))
    return results


@router.post("/rrf")
async def rrf(request: schemas.FTSQuerySchema):
    query = request
    query = query.query
    arg=request.arg
    full_text_penalty = 1
    
    payload={
        "query":query
    }
    payload2={
        "query":query,
        "arg":arg
    }
    # print(payload,"  hello Harsh  ",payload2)
    # print("Towards Output!")
    resultVs = await sem_search(schemas.RRFQuerySchema(query=query))
    # print(resultVs)
    resultFts=await fts_search(schemas.FTSQuerySchema(query=query,arg=arg))
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
            response.append(result)    
            
    response.sort(reverse=True,key=get_score)   
    return response


