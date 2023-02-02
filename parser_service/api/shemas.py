from pydantic import BaseModel

#########################
# BLOCK WITH API MODELS #
#########################


class TunedModel(BaseModel):
    class Config:
        """tells pydantic to convert even non dict obj to json"""

        orm_mode = True


class ShowCategory(TunedModel):
    pk: int
    id: int
    name: str
    parent_id: int
    shard: str
    query: str
    url: str
    children: bool
