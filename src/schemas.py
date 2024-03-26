from pydantic import BaseModel
from typing import Optional


class MovieEmbedSchema(BaseModel):
    _id: str
    title: str
    plot: str

class RRFQuerySchema(BaseModel):
    query: str
    
class FilterSchema(BaseModel):
    query:str
    genre:Optional[str]=None
    year:Optional[int]=None
    language:Optional[str]=None
