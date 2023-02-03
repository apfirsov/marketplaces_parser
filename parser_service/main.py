import sys
import logging
import uvicorn
from fastapi import FastAPI
from fastapi.routing import APIRouter

from api.handlers import parser_router
from launcher import launcher

#########################
# BLOCK WITH API ROUTES #
#########################

# create instance of the app
app = FastAPI(title="parser_marketplace")

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
    func_name = sys.argv[1:]
    if func_name:
        try:
            logging.info(
                f"Запуск дополнительной функции с ключом {func_name[0]}"
            )
            launcher(func_name[0])
        except Exception as e:
            logging.exception(f"ошибка ввода {e}")
    else:
        uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format=(
            '%(asctime)s [%(levelname)s] - %(message)s'
        ),
        handlers=[
            logging.FileHandler('logfile.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    main()
