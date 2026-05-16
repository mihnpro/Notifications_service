"""
Скрипт для наполнения базы 50 000 тестовых пользователей и их каналов связи.
Запускать после применения миграций (make migrate).

Требует переменную окружения DATABASE_URL.
"""

import os
import sys
import psycopg2
from psycopg2 import sql

# ------------------------------------------------------------------
# 1. Получение и преобразование URL базы
# ------------------------------------------------------------------
RAW_DATABASE_URL = os.getenv("DATABASE_URL")
if not RAW_DATABASE_URL:
    print("FATAL: DATABASE_URL is not set", file=sys.stderr)
    sys.exit(1)

# Превращаем асинхронный URL в синхронный (как в env.py)
DATABASE_URL = RAW_DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

# ------------------------------------------------------------------
# 2. SQL-шаблоны
# ------------------------------------------------------------------

# Вставка базовых каналов, если они ещё не существуют
UPSERT_CHANNELS_SQL = """
    INSERT INTO channels (code, display_name, state, queue_group, adapter_name)
    VALUES
        ('email', 'Email', 'enabled', 'email', 'stub'),
        ('sms', 'SMS', 'enabled', 'sms', 'stub'),
        ('telegram', 'Telegram', 'enabled', 'messenger', 'stub')
    ON CONFLICT (code) DO NOTHING;
"""

# Получение ID каналов по кодам
GET_CHANNEL_IDS_SQL = """
    SELECT code, id FROM channels WHERE code IN ('email', 'sms', 'telegram');
"""

# Вставка 50 000 пользователей и их каналов одним запросом (CTE)
SEED_USERS_AND_CHANNELS_SQL = """
    WITH new_users AS (
        INSERT INTO users (region_id, external_id, status)
        SELECT 'default', NULL, 'active'
        FROM generate_series(1, 50000)
        ON CONFLICT DO NOTHING
        RETURNING id
    ),
    channels AS (
        SELECT id, code FROM channels
        WHERE code IN ('email', 'sms', 'telegram')
    )
    INSERT INTO user_channels (region_id, user_id, channel_id, address, status, verified)
    SELECT
        'default',
        u.id,
        c.id,
        replace(u.id::text, '-', '') || '@example.com',
        'active',
        true
    FROM new_users u
    CROSS JOIN channels c
    ON CONFLICT (user_id, channel_id, address) DO NOTHING;
"""

# ------------------------------------------------------------------
# 3. Вспомогательные функции
# ------------------------------------------------------------------
def execute(cursor, query, description):
    """Выполнить запрос и вывести количество затронутых строк."""
    try:
        cursor.execute(query)
        count = cursor.rowcount
        print(f"{description}: затронуто строк = {count}")
    except Exception as exc:
        print(f"ERROR при выполнении '{description}': {exc}", file=sys.stderr)
        raise

def ensure_channels(cursor):
    """Создать базовые каналы и вернуть словарь code -> id."""
    execute(cursor, UPSERT_CHANNELS_SQL, "Вставка каналов (email, sms, telegram)")
    cursor.execute(GET_CHANNEL_IDS_SQL)
    channels = {row[0]: row[1] for row in cursor.fetchall()}
    print(f"Используются каналы: {channels}")
    return channels

# ------------------------------------------------------------------
# 4. Главная логика
# ------------------------------------------------------------------
def main():
    print("Подключение к PostgreSQL...")
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        cur = conn.cursor()

        # Шаг 1. Убедиться, что каналы есть
        channel_ids = ensure_channels(cur)

        # Шаг 2. Массово создать пользователей и их каналы
        execute(cur, SEED_USERS_AND_CHANNELS_SQL, "Пользователи (50k) + каналы (email/sms/telegram)")

        conn.commit()
        print("База данных успешно наполнена.")

    except Exception:
        print("Ошибка, выполняется откат транзакции.", file=sys.stderr)
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Соединение закрыто.")

if __name__ == "__main__":
    main()