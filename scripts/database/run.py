import os
from db_main import EasyNerDBHandler  # Ensure db_main is imported
import db_statistics  # Ensure db_statistics is imported
from IPython import embed

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Create (or connect to) the database.
    db = EasyNerDBHandler(db_path=os.path.join(script_dir, "database.db"))
    data_dir = "/lunarc/nobackup/projects/snic2020-6-41/carl"
    # db_path = f"{data_dir}/pubmed_abstracts3.db"
    # db = EasyNerDBHandler(db_path)
    print("Entering interactive mode. The 'db' variable is available.")

    # Drop into an interactive shell with local variables available.
    # import code

    # code.interact(local=locals())

    embed()
