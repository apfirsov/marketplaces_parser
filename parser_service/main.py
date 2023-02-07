# import logging

import uvicorn
from fastapi import FastAPI
from fastapi.routing import APIRouter
from logger_config import parser_logger as logger

from api.handlers import parser_router

#########################
# BLOCK WITH API ROUTES #
#########################


# create instance of the app
def create_app() -> FastAPI:
    app = FastAPI(title='Parser_marketplace', debug=False)
    app.logger = logger
    return app


app = create_app()

# create the instance for the routes
main_api_router = APIRouter()

# set routes to the app instance
main_api_router.include_router(
    parser_router,
    prefix="/api",
    tags=["parserAPI"]
)
app.include_router(main_api_router)


def main():
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    except Exception as e:
        logger.exception(f"uvicorn faild {e}")


if __name__ == "__main__":
    main()
