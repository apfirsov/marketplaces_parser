from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from db.dals import CategoryDAL
from db.models import Category
from db.session import get_db

#########################
# BLOCK WITH API ROUTES #
#########################

parser_router = APIRouter()


@parser_router.get("/categories")
async def get_categories_list(
    db: AsyncSession = Depends(get_db)
) -> list[Category]:
    async with db as session:
        async with session.begin():
            parser_dal = CategoryDAL(session)
            return await parser_dal.get_all_items()
