from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
)
from sqlalchemy.orm import declarative_base

##############################
# BLOCK WITH DATABASE MODELS #
##############################


Base = declarative_base()


class Category(Base):
    __tablename__ = "Category"

    id = Column(
        "id",
        Integer,
        primary_key=True
    )
    name = Column(
        "name",
        String,
    )
    parent_id = Column(
        "parent_id",
        Integer
    )
    shard = Column(
        "category_shard",
        String
    )
    query = Column(
        "query",
        String
    )
    url = Column(
        "url",
        String
    )
    children = Column(
        "children",
        Boolean
    )
    goods_displayed = Column(
        "displayed",
        Boolean
    )
