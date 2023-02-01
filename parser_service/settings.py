"""File with settings and configs for the project"""
import os
from dotenv import load_dotenv

load_dotenv()

REAL_DATABASE_URL = os.environ.get(
    "ASYNC_DATABASE_URL",
    default="postgresql+asyncpg://postgres:postgres@0.0.0.0:5432/postgres",
)
