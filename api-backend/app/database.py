import os
import psycopg2
from contextlib import contextmanager


def get_connection_params():
    """Parâmetros de conexão do PostgreSQL."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB", "b3_data"),
        "user": os.getenv("POSTGRES_USER", "user"),
        "password": os.getenv("POSTGRES_PASSWORD", "password"),
        "sslmode": os.getenv("POSTGRES_SSL_MODE", "require"),
    }


@contextmanager
def get_db():
    """Context manager de conexão PostgreSQL."""
    conn = None
    try:
        conn = psycopg2.connect(**get_connection_params())
        yield conn
    finally:
        if conn:
            conn.close()
