import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from forwarder.scripts.run import main

if __name__ == "__main__":
    main()
