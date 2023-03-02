import sys
from pathlib import Path

parent_path = str(Path(__file__).parents[1])

if parent_path not in sys.path:
    sys.path.insert(0, parent_path)

if __name__ == "__main__":
    from launcher import main

    main(sys.argv)
