from loader import categories

COMMANDS = {
        "-c": categories.load_all_items,
    }


def launcher(command):
    return COMMANDS[command]()
