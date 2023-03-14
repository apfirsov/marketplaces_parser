import uvicorn
from fastapi import FastAPI
from fastapi.routing import APIRouter

from api.handlers import parser_router
from logger_config import parser_logger as logger
from settings import API_PREFIX, API_TITLE, HOST, PORT, ROUTER_TAGS


def create_app() -> FastAPI:
    app = FastAPI(title=API_TITLE, debug=False)
    app.logger = logger
    return app


app = create_app()

main_api_router = APIRouter()

main_api_router.include_router(
    parser_router,
    prefix=API_PREFIX,
    tags=ROUTER_TAGS
)
app.include_router(main_api_router)


def main():
    try:
        uvicorn.run(app, host=HOST, port=PORT)
    except Exception as e:
        logger.exception("uvicorn failed %s ", e)


if __name__ == "__main__":
    main()
