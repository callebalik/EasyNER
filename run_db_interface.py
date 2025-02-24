from scripts.database.db_main import EasyNerDBHandler, EasyNERDB  # Ensure db_main is imported
from IPython import embed
# import code

if __name__ == "__main__":
    db = EasyNerDBHandler()
    # test = EasyNERDB()
    print("Entering interactive mode. The 'db' variable is available for database interaction.")

    # Drop into an interactive shell with local variables available.
    # code.interact(local=locals())
    embed() # Embed IPython shell, provides autocomplete and other features