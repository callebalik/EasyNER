from db_main import EasyNerDBHandler  # Ensure db_main is imported

db = EasyNerDBHandler()
db.analysis.create_temp_normalized_entities()