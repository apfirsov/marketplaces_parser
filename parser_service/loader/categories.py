import json

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from db.models import Category
from settings import POSTGRES_URL

engine = create_engine(
    POSTGRES_URL,
    future=True,
    echo=True,
    execution_options={"isolation_level": "AUTOCOMMIT"},
)
s = Session(bind=engine)


def load_all_items() -> None:
    with open("loader/category.json", "r") as file:
        objects = json.load(file)
    s.bulk_insert_mappings(
        Category,
        objects
    )
    s.commit()
