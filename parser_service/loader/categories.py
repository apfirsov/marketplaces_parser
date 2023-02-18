import sys
from contextlib import contextmanager
from http import HTTPStatus
from typing import Optional

# import db.session
import requests
from fastapi import Depends
from fastapi.concurrency import contextmanager_in_threadpool
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Category
from db.session import get_db
from logger_config import parser_logger as logger

from .constants import MAIN_MENU
from .exceptions import EmptyResponseError, ResponseStatusCodeError
from .schemas import SourceCategory
from contextlib import contextmanager


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


async def load_all_items() -> None:
    # db = get_db()
    # session: AsyncSession = await anext(db)
    # db: AsyncSession = Depends(get_db)
    # session = db.session.get_db()

    catalogue_url: str = MAIN_MENU
    try:
        response = requests.get(catalogue_url)

        if response.status_code != HTTPStatus.OK:
            raise ResponseStatusCodeError()

        response_json: list[dict] = response.json()

        if not len(response_json):
            raise EmptyResponseError()

        objects: list[dict] = _handle_response(response_json)

        inst_lst = [Category(**dct) for dct in objects]

    except Exception as error:
        logger.exception(error)
        sys.exit()
    # print(db)

    # async with db as session:
    # async with contextmanager(get_db)() as session:
    async with contextmanager_in_threadpool(
        contextmanager(get_db)()) as session:
        print("session")
        async with session.begin():
            await session.execute(delete(Category))
            session.add_all(inst_lst)
            await session.commit()
