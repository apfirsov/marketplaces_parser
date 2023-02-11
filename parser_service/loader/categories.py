import sys
from typing import Optional

import requests
from http import HTTPStatus
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from db.models import Category
from settings import POSTGRES_URL

from .constants import MAIN_MENU
from .exceptions import EmptyResponseError, ResponseStatusCodeError
from logger_config import parser_logger as logger
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
        landing: Optional[list[dict]] = item.get('landing')
        parent: Optional[list[dict]] = item.get('parent')
        if landing or parent:
            section = SourceCategory(**item)
            result.append(section.dict())
        if childs:
            result.extend(_handle_response(childs))
    return result


def load_all_items() -> None:
    catalogue_url: str = MAIN_MENU
    try:
        try:
            response = requests.get(catalogue_url)

            if response.status_code != HTTPStatus.OK:
                raise ResponseStatusCodeError()

            response_json: list[dict] = response.json()

            if not len(response_json):
                raise EmptyResponseError()

        except requests.exceptions.JSONDecodeError as error:
            raise error

        try:
            objects: list[dict] = _handle_response(response_json)
        except ValidationError as error:
            raise error

    except Exception as error:
        logger.exception(error)
        sys.exit()

    with Session(engine) as s:
        s.query(Category).delete()
        s.bulk_insert_mappings(
            Category,
            objects
        )
        s.commit()
