import datetime
from sqlalchemy import Boolean, Column, Integer, String, ForeignKey, DateTime, Float, Numeric, Table
from sqlalchemy.orm import declarative_base, relationship

##############################
# BLOCK WITH DATABASE MODELS #
##############################


Base = declarative_base()


class Category(Base):
    __tablename__ = "category"

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
    url = Column(
        "url",
        String
    )
    shard = Column(
        "category_shard",
        String
    )
    query = Column(
        "query",
        String
    )
    childs = Column(
        "children",
        Boolean
    )


class Brand(Base):
    __tablename__ = "brands"

    id = Column(
        "id",
        Integer,
        primary_key=True
    )
    name = Column(
        "name",
        String
    )


class Color(Base):
    __tablename__ = "colors"

    id = Column(
        "id",
        Integer,
        primary_key=True
    )
    name = Column(
        "name",
        String
    )


class Good(Base):
    __tablename__ = "goods"

    id = Column(
        "id",
        Integer,
        primary_key=True
    )
    catalogue_id = Column(
        "catalogue_id",
        Integer,
        ForeignKey("category.id")
    )
    name = Column(
        "name",
        String
    )
    brand_id = Column(
        "brand_id",
        Integer,
        ForeignKey("brands.id")
    )
    color_id = Column(
        "color_id",
        Integer,
        ForeignKey("colors.id")
    )


class Size(Base):
    __tablename__ = "sizes"

    id = Column(
        "id",
        Integer,
        primary_key=True
    )
    name = Column(
        "name",
        String
    )


class GoodsHistorySize(Base):
    __tablename__ = "goods_history_size"

    id = Column(
        "id",
        Integer,
        primary_key=True
    )
    history_id = Column(
        "history_id",
        ForeignKey("goods_history.id"),
        primary_key=True
    )
    size_id = Column(
        "size_id",
        ForeignKey("sizes.id"),
        primary_key=True
    )
    amount = Column(
        "amount",
        Integer
    )
    # size = relationship("Size")


# GoodsHistorySize = Table(
#     "goods_history_size",
#     Base.metadata,
#     Column(
#         "id",
#         Integer,
#         primary_key=True
#     ),
#     Column(
#         "history_id",
#         ForeignKey("goods_history.id")
#     ),
#     Column(
#         "size_id",
#         ForeignKey("sizes.id")
#     ),
#     Column(
#         "amount",
#         Integer
#     ),
# )


class GoodsHistory(Base):
    __tablename__ = "goods_history"

    id = Column(
        "id",
        Integer,
        primary_key=True
    )
    good_id = Column(
        "good_id",
        Integer,
        ForeignKey("goods.id")
    )
    timestamp = Column(
        "timestamp",
        DateTime
    )
    sale = Column(
        "sale",
        Float # возможно Numeric 
    )
    price_full = Column(
        "price_full",
        Integer
    )
    price_with_discount = Column(
        "price_with_discount",
        Integer
    )
    rating = Column(
        "rating",
        Float
    )
    feedback = Column(
        "feedback",
        Integer
    )
    sizes = relationship(
        "GoodsHistorySize"
    )
    # sizes = relationship(
    #     "Size",
    #     secondary=GoodsHistorySize
    # )
