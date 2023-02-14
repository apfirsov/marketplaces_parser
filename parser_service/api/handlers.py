from db.dals import CategoryDAL, GoodsHystoryDAL
from db.models import Category, GoodsHistory
from db.session import get_db
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

parser_router = APIRouter()


@parser_router.get("/categories")
async def get_categories_list(
    db: AsyncSession = Depends(get_db)
) -> list[Category]:
    async with db as session:
        async with session.begin():
            parser_dal = CategoryDAL(session)
            return await parser_dal.get_all_items()


@parser_router.get("/goods_history")
async def get_goods_history_list(
    db: AsyncSession = Depends(get_db)
) -> list[GoodsHistory]:
    async with db as session:
        async with session.begin():
            parser_dal = GoodsHystoryDAL(session)
            return await parser_dal.get_all_items()
