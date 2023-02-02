from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.orm import declarative_base

##############################
# BLOCK WITH DATABASE MODELS #
##############################


Base = declarative_base()


class Category(Base):
    __tablename__ = "Category"

    pk = Column(
        "id",
        Integer,
        primary_key=True
    )
    id = Column(
        "id",
        Integer
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
