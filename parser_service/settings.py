import os

from dotenv import load_dotenv

load_dotenv()

POSTGRES_URL = os.environ.get(
    "POSTGRES_URL",
    default="postgresql+asyncpg://postgres:postgres@0.0.0.0:5432/postgres",
)

HOST = "0.0.0.0"

PORT = 8000

API_TITLE = 'Parser_marketplace'

ROUTER_TAGS = ["parserAPI"]

API_PREFIX = "/api"
