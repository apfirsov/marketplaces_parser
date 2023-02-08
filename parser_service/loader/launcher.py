from .categories import load_all_items
from logger_config import parser_logger as logger

LAUNCH_OPTIONS = {
    "start": {
        "--categories": load_all_items,
    },
}


def main(argv=None):
    if argv is None:
        logger.debug("argv is None")
        return

    func_name = argv[1:]
    try:
        logger.info(
            f"start launcher with param: {func_name[0]} {func_name[1]}"
        )
        LAUNCH_OPTIONS[func_name[0]][func_name[1]]()
    except Exception as error:
        logger.exception(f"launcher failed: {error}")


if __name__ == "__main__":
    main()
