import sys
from typing import Optional

import requests
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from db.models import Category
from settings import POSTGRES_URL

from .constants import MAIN_MENU
from .logger_file_conf import parser_logger as logger
from .schemas import SourceCategory

engine = create_engine(
    POSTGRES_URL,
    future=True,
    echo=True,
    execution_options={"isolation_level": "AUTOCOMMIT"},
)


def _handle_response(response: list[dict]) -> list[dict]:
    result: list[dict] = []
    for item in response:
        childs: Optional[list[dict]] = item.get('childs')
        section = SourceCategory(**item)
        if childs:
            result.extend(_handle_response(childs))
        result.append(section.dict())
    return result


def load_all_items() -> None:
    catalogue_url: str = MAIN_MENU
    try:
        try:
            response: list[dict] = requests.get(catalogue_url).json()
            objects: list[dict] = _handle_response(response)
        except ValidationError as error:
            raise error
    except Exception as error:
        logger.error(error, exc_info=True)
        sys.exit()

    with Session(engine) as s:
        s.bulk_insert_mappings(
            Category,
            objects
        )
        s.commit()
