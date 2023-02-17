from sqlalchemy import (Boolean, Column, DateTime, Float, ForeignKey, Integer,
                        String)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Category(Base):
    __tablename__ = "categories"

    id = Column(Integer, primary_key=True)
    name = Column(String,)
    parent = Column(Integer, ForeignKey("categories.id"))
    url = Column(String)
    shard = Column(String)
    query = Column(String)
    children = Column(Boolean)


class Brand(Base):
    __tablename__ = "brands"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class Color(Base):
    __tablename__ = "colors"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True)
    category = Column(Integer, ForeignKey("categories.id"))
    name = Column(String)
    brand = Column(Integer, ForeignKey("brands.id"))
    color = Column(Integer, ForeignKey("colors.id"))


class Size(Base):
    __tablename__ = "sizes"

    id = Column(Integer, primary_key=True)
    name = Column(String)


class HistorySizeRelation(Base):
    __tablename__ = "history_size_relations"

    history = Column(ForeignKey("items_history.id"), primary_key=True)
    size = Column(ForeignKey("sizes.id"), primary_key=True)
    count = Column(Integer)


class ItemsHistory(Base):
    __tablename__ = "items_history"

    id = Column(Integer, primary_key=True)
    item = Column(Integer, ForeignKey("items.id"))
    timestamp = Column(DateTime)
    sale = Column(Integer)
    price_full = Column(Integer)
    price_with_discount = Column(Integer)
    rating = Column(Integer)
    feedbacks = Column(Integer)
    sizes = relationship("HistorySizeRelation")
    sum_count = Column(Integer)
