from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from notifications_api.adapters.postgres_models import Base
from notifications_api.infra import GlobalConfig

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_db_url() -> str:
    cfg = GlobalConfig.load()
    return cfg.database_url.replace("postgresql+asyncpg://", "postgresql://")


def run_migrations_offline() -> None:
    url = get_db_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    db_url = get_db_url()
    connectable = engine_from_config(
        {"sqlalchemy.url": db_url},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
