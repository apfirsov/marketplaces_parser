from sqlalchemy import (
    Boolean, Column, Integer,
    String, ForeignKey, DateTime, Float
)
from sqlalchemy.orm import declarative_base, relationship

##############################
# BLOCK WITH DATABASE MODELS #
##############################


Base = declarative_base()


class Category(Base):
    __tablename__ = "categories"

    id = Column(Integer, primary_key=True)
    name = Column(String,)
    parent_id = Column(Integer)
    url = Column(String)
    shard = Column(String)
    query = Column(String)
    childs = Column(Boolean)


class Brand(Base):
    __tablename__ = "brands"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class Color(Base):
    __tablename__ = "colors"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class Good(Base):
    __tablename__ = "goods"

    id = Column(Integer, primary_key=True)
    catalogue_id = Column(Integer, ForeignKey("categories.id"))
    name = Column(String)
    brand_id = Column(Integer, ForeignKey("brands.id"))
    color_id = Column(Integer, ForeignKey("colors.id"))


class Size(Base):
    __tablename__ = "sizes"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class GoodsHistorySize(Base):
    __tablename__ = "goods_history_size"

    id = Column(Integer, primary_key=True)
    history_id = Column(ForeignKey("goods_history.id"), primary_key=True)
    size_id = Column(ForeignKey("sizes.id"), primary_key=True)
    amount = Column(Integer)


class GoodsHistory(Base):
    __tablename__ = "goods_history"

    id = Column(Integer, primary_key=True)
    good_id = Column(Integer, ForeignKey("goods.id"))
    timestamp = Column(DateTime)
    sale = Column(Float)
    price_full = Column(Integer)
    price_with_discount = Column(Integer)
    rating = Column(Float)
    feedbacks_count = Column(Integer)
    sizes = relationship("GoodsHistorySize")
