from pydantic import BaseModel


#########################
# BLOCK WITH API MODELS #
#########################


class TunedModel(BaseModel):
    class Config:
        """tells pydantic to convert even non dict obj to json"""

        orm_mode = True


class ShowCategory(TunedModel):
    id: int
    name: str
    parent_id: int
    shard: str
    query: str
    url: str  # HttpUrl
    children: bool
    goods_displayed: bool