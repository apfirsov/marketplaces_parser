from typing import List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Category


class CategoryDAL:
    """Data Access Layer for operating parser info"""
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def get_all_items(self) -> List[Category]:
        query = await self.db_session.execute(
            select(Category).order_by(Category.id)
        )
        return query.scalars().all()
