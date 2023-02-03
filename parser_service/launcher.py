from loader import categories

COMMANDS = {
        "-c": categories.load_all_items,
        "-t": categories.test
    }


def launcher(command):
    return COMMANDS[command]()
