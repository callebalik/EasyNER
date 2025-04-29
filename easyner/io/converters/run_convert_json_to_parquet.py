# Basic usage with default settings
from easyner.io.converters.converters import convert_json_to_parquet


stats = convert_json_to_parquet(
    json_dir="/lunarc/nobackup/projects/snic2020-6-41/carl/data/ner_output",
    parquet_dir="/lunarc/nobackup/projects/snic2020-6-41/carl/data/parquet/",
)
print(f"Conversion stats: {stats}")
