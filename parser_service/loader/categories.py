import sys
from typing import Optional
# import asyncio
import requests
from http import HTTPStatus
# from sqlalchemy import create_engine
# from sqlalchemy.orm import Session

from db.models import Category
from logger_config import parser_logger as logger
from pydantic import ValidationError
# from settings import POSTGRES_URL, REAL_DATABASE_URL
# from sqlalchemy import create_engine
# from sqlalchemy.orm import Session
# from db.session import engine
from db.session import get_db
# from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import delete

from .constants import MAIN_MENU
from .schemas import SourceCategory

from .exceptions import EmptyResponseError, ResponseStatusCodeError
from logger_config import parser_logger as logger
from .schemas import SourceCategory


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
    db = get_db()
    session: AsyncSession = await anext(db)

    catalogue_url: str = MAIN_MENU
    try:
        response = requests.get(catalogue_url)

        if response.status_code != HTTPStatus.OK:
            raise ResponseStatusCodeError()

        response_json: list[dict] = response.json()

        if not len(response_json):
            raise EmptyResponseError()

        objects: list[dict] = _handle_response(response_json)

    except Exception as error:
        logger.exception(error)
        sys.exit()

    inst_lst = [Category(**dct) for dct in objects]

    async with session.begin():
        await session.execute(delete(Category))
        await session.add_all(inst_lst)
        await session.commit()
