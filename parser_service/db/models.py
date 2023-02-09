from sqlalchemy import Boolean, Column, Integer, String, ForeignKey
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


class Brands(Base):
    __tablename__ = "Brands"

    id = Column(
        "id",
        Integer,
        primary_key=True
    )
    name = Column(
        "name",
        String
    )


class Colors(Base):
    __tablename__ = "Colors"

    id = Column(
        "id",
        Integer,
        primary_key=True
    )
    name = Column(
        "name",
        String
    )


class Goods(Base):
    __tablename__ = "Goods"

    id = Column(
        "id",
        Integer,
        primary_key=True
    )
    catalogue_id = Column(
        "catalogue_id",
        Integer,
        ForeignKey("Category.id")
    )
    name = Column(
        "name",
        String
    )
    brand_id = Column(
        "brand_id",
        Integer,
        ForeignKey("Brands.id")
    )
    color_id = Column(
        "color_id",
        Integer,
        ForeignKey("Colors.id")

    )
