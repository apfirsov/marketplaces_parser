from datetime import datetime
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
    url: str
    children: bool


# class SwowGoodsHistory(TunedModel):
#     id: int
#     good_id: int = None
#     timestamp: datetime = None
#     sale: float = None
#     price_full: int = None
#     price_with_discount: int = None
#     rating: float = None
#     feedbacks_count: int = None
#     sizes: list = None
