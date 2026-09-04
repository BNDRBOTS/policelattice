from __future__ import annotations

from app import models  # noqa: F401
from app.db import init_database_with_retry


def main() -> None:
    init_database_with_retry()
    print("Database schema verified and initialized.")


if __name__ == "__main__":
    main()
