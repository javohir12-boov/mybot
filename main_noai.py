import asyncio
import os

# Force NO-AI mode without requiring .env edits.
os.environ["BOT_MODE"] = "noai"

from main import main  # noqa: E402


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped")
