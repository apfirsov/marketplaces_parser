from .categories import load_all_items
from .logger_file_conf import parser_logger as logger

LAUNCH_OPTIONS = {
    "start": {
        "--categories": load_all_items,
    },
}


def main(argv=None):
    if argv is None:
        logger.debug(
            f"argv =  {argv}"
        )
        return

    func_name = argv[1:]
    try:
        logger.info(
            f"start launcher with param: {func_name[0]} {func_name[1]}"
        )
        LAUNCH_OPTIONS[func_name[0]][func_name[1]]()
    except Exception as e:
        logger.exception(f"launcher faild: {e}")


if __name__ == '__main__':
    main()
