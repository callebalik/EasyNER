from scripts.database.db_main import EasyNerDBHandler
from IPython import embed

if __name__ == "__main__":
    with EasyNerDBHandler() as db:
        print(
            "Entering interactive mode. The 'db' variable is available for database interaction."
        )
        # Drop into an interactive shell with local variables available.
        embed()  # Embed IPython shell, provides autocomplete and other features
