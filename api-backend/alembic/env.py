from logging.config import fileConfig
import os
from sqlalchemy import engine_from_config
from sqlalchemy import pool
from alembic import context

# Configuração do Alembic
config = context.config

# Sobrescreve a URL de conexão com variáveis de ambiente
config.set_main_option(
    'sqlalchemy.url',
    f"postgresql://{os.getenv('POSTGRES_USER', 'user')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'password')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'b3_data')}"
)

# Configuração de logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Metadados do modelo para autogeração de migrações
target_metadata = None


def run_migrations_offline() -> None:
    """Roda migrações em modo 'offline'."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Roda migrações em modo 'online'."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
