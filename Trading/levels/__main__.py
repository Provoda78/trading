import asyncio
import os
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from Trading.levels.main import main
else:
    from .main import main


if __name__ == "__main__":
    asyncio.run(main())
