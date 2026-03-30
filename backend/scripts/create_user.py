from __future__ import annotations

import os
import sys

os.environ.setdefault("APP_DB_BACKEND", "postgres")

from backend.services.store_data_model import init_store_data_model
from backend.services.auth_service import create_or_update_user


def main() -> int:
    identifier = str(sys.argv[1] if len(sys.argv) > 1 else "maksim").strip()
    password = str(sys.argv[2] if len(sys.argv) > 2 else "").strip()
    if not password:
        raise SystemExit("password required")
    display_name = str(sys.argv[3] if len(sys.argv) > 3 else identifier).strip()
    role = str(sys.argv[4] if len(sys.argv) > 4 else "owner").strip() or "owner"
    init_store_data_model()
    user = create_or_update_user(
        identifier=identifier,
        password=password,
        display_name=display_name,
        role=role,
        is_active=True,
    )
    print(f"created {user['identifier']} ({user['role']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
