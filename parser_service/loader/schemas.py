import re
from typing import Optional

from pydantic import BaseModel, validator


class SourceCategory(BaseModel):
    id: int
    name: str
    parent_id: Optional[int]
    url: str
    shard: Optional[str]
    query: Optional[str]
    childs: Optional[list[dict]]

    @validator('query')
    def validate_query_prefix(cls, v):
        if v is not None and '=' not in v:
            raise ValueError('query must contain "="')
        return v

    @validator('url')
    def validate_url(cls, v):
        if not (v.startswith('/') or v.startswith('https://')):
            raise ValueError('url must start with "/" or "https://"')
        return v

    @validator('shard', 'query')
    def validate_cyrillic(cls, v):
        if v is not None and re.search('[а-яА-Я]', v):
            raise ValueError('this field must not contain cyrillic letters')
        return v

    @validator('shard', 'query')
    def validate_spaces(cls, v):
        if v is not None and ' ' in v:
            raise ValueError('this field must not contain spaces')
        return v