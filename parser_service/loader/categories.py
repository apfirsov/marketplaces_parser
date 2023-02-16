import sys
from typing import Optional
import asyncio
import requests
from db.models import Category
from logger_config import parser_logger as logger
from pydantic import ValidationError
from settings import POSTGRES_URL, REAL_DATABASE_URL
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from db.session import engine
from db.session import get_db
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from .constants import MAIN_MENU
from .schemas import SourceCategory

# engine = create_engine(
#     POSTGRES_URL,
#     future=True,
#     echo=True,
#     execution_options={"isolation_level": "AUTOCOMMIT"},
# )


def _handle_response(response: list[dict]) -> list[dict]:
    result: list[dict] = []
    for item in response:
        childs: Optional[list[dict]] = item.get('childs')
        section = SourceCategory(**item)
        if childs:
            result.extend(_handle_response(childs))
        result.append(section.dict())
    return result


async def load_all_items() -> None:
    db: AsyncSession = Depends(get_db)
    
    catalogue_url: str = MAIN_MENU
    try:
        try:
            response: list[dict] = requests.get(catalogue_url).json()
            objects: list[dict] = _handle_response(response)
        except ValidationError as error:
            raise error
    except Exception as error:
        logger.exception(error)
        sys.exit()

    async with db as session:
        async with session.begin():
            await session.query(Category).delete()
            await session.bulk_insert_mappings(
                Category,
                objects
            )
            await session.commit()


# async def test() -> None:
#     print("test")
#     await asyncio.sleep(1)
#     print("test")
