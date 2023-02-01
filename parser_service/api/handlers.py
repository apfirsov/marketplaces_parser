from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from db.dals import CategoryDLA
from db.models import Category
from db.session import get_db

#########################
# BLOCK WITH API ROUTES #
#########################

parser_router = APIRouter()


@parser_router.get("/category")
async def get_category(
    db: AsyncSession = Depends(get_db)
) -> List[Category]:
    async with db as session:
        async with session.begin():
            parser_dal = CategoryDLA(session)
            return await parser_dal.get_all_items()
