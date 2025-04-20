import json
from pathlib import Path

DB_PATH = Path("database/users_db.json")


def save_users_db(users_db: dict[int, dict[str, list[tuple[str, str]]]]) -> None:
    """Сохраняет users_db в JSON-файл"""
    serializable_db = {
        str(user_id): {
            key: list(map(list, value))  # tuple -> list
            for key, value in inner.items()
        }
        for user_id, inner in users_db.items()
    }

    with DB_PATH.open("w", encoding="utf-8") as f:
        json.dump(serializable_db, f, ensure_ascii=False, indent=2)


def load_users_db() -> dict[int, dict[str, list[tuple[str, str]]]]:
    """Загружает users_db из JSON-файла"""
    if not DB_PATH.exists() or DB_PATH.stat().st_size == 0:
        return {}

    with DB_PATH.open("r", encoding="utf-8") as f:
        raw_data: dict[str, dict[str, list[list[str]]]] = json.load(f)

    deserialized_db = {
        int(user_id): {
            key: [tuple(item) for item in value]
            for key, value in inner.items()
        }
        for user_id, inner in raw_data.items()
    }

    return deserialized_db