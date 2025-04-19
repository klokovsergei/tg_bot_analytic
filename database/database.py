from services.storage import load_users_db

user_dict_template: dict[str, list[tuple[str, str]]] = {
    'user_usage_ym_transformer': []
    , 'temp_file': []
}

# Инициализируем "базу данных"
users_db: dict[int, dict[str, list[tuple[str, str]]]] = load_users_db()
