from IPython import embed

from easyner.database.sqlite_backend.db_main import EasyNerDBHandler

if __name__ == "__main__":
    with EasyNerDBHandler() as db:
        print(
            "Entering interactive mode. The 'db' variable is available for database interaction.",
        )
        # Drop into an interactive shell with local variables available.
        embed()  # Embed IPython shell, provides autocomplete and other features
