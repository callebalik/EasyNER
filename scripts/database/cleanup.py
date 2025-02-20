from db_main import EasyNerDBHandler

if __name__ == "__main__":
    db = EasyNerDBHandler()

    # Rename named entities for easier analysis
    # db.data_exchanger.rename_named_entity("phenomenon", "PNM")
    # db.data_exchanger.rename_named_entity("disease", "DIS")


    # db.data_cleaner.clean_inclusive_entity_spans("PNM")



    # Perform analysis
    db.data_cleaner.set_error_entity_error_codes()
    