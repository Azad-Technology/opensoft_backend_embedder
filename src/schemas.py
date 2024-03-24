from pydantic import BaseModel


class MovieEmbedSchema(BaseModel):
    _id: str
    title: str
    plot: str

class RRFQuerySchema(BaseModel):
    query: str
    
class FTSQuerySchema(BaseModel):
    query:str
    arg:str
