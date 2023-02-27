from pydantic import BaseModel


class TunedModel(BaseModel):
    class Config:
        """tells pydantic to convert even non dict obj to json"""

        orm_mode = True


class ShowCategory(TunedModel):
    id: int
    name: str
    parent: int
    url: str
    shard: str
    query: str
    children: bool
