from alembic.config import Config
from alembic import command
from src.config import settings


def run_migrations():
    cfg = Config()
    cfg.set_main_option("script_location", 'alembic_folder')
    cfg.set_main_option("sqlalchemy.url",
                        f'postgresql://{settings.pg_user}:{settings.pg_pass}@{settings.pg_host}:{settings.pg_port}/{settings.pg_db}')

    command.upgrade(cfg, "head")


if __name__ == "__main__":
    run_migrations()