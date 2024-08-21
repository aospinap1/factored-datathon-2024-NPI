import sys
import os

main_dir = os.path.dirname(sys.path[0])
paths = {
    "src": os.path.join(main_dir, "src"),
    "data": os.path.join(main_dir, "data"),
    "config": os.path.join(main_dir, "config")
}
