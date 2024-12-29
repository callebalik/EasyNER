# coding=utf-8

import json
import os
import re
from tqdm import tqdm, trange
from glob import glob
import logging

# Add: Entiry Merge can now handle file mismatch (implemented for 2 directories)

# 1. Looks for correctly formatted files in the input directories
# 2. Looks for files with same batch number

# If no vaild matched file in accodance with 1, 2 dosen't merge file and logs as not merged to results/entity_merger_log

# If there are fields that should be retained but are not present in one set of files, the path to the articles with the fields should be the first in the list of paths in the config.

# Set up logging to file
logging.basicConfig(
    filename="results/entity_merger_log.txt",
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def read_articles(filename: str):
    try:
        with open(filename, encoding="utf-8") as f:
            return json.loads(f.read())
    except Exception as e:
        logging.error(f"Error reading file {filename}: {e}")
        return None


def get_sorted_files(filepath):
    """
    get a list of sorted file paths using glob
    """
    return sorted(
        glob(f"{filepath}*.json"),
        key=lambda x: int(os.path.splitext(os.path.basename(x))[0].split("-")[-1]),
    )


def process_articles(articles: dict, entity_tag: str):
    """
    process the article to contain tag
    """
    for art in articles:  # Iterate directly over the dictionary keys
        for i, sent in enumerate(articles[art]["sentences"]):  # Use enumerate to get both index and sentence
            if sent["entities"]:  # Check if there are entities in the sentence
                # Directly update the sentence using the sent variable for readability
                sent["entities"] = {
                    entity_tag: sent["entities"]
                }
                sent["entity_spans"] = {
                    entity_tag: sent["entity_spans"]
                }
            else:
                # If no entities, set empty dictionaries
                sent["entities"] = {}
                sent["entity_spans"] = {}
    return articles  # Return the updated articles dictionary

def merge_two_articles(articles_1, articles_2):
    """
    merge articles_2 with articles_1 at entity level
    """
    if len(articles_1) == 0:
        articles_1 = {k: v for k, v in articles_2.items()}
        return articles_1

    elif len(articles_2) == 0 and len(articles_1) > 0:  # user exception
        articles_2 = {k: v for k, v in articles_1.items()}
        return articles_2

    else:
        for art1, art2 in zip(list(articles_1), list(articles_2)):
            if art1 != art2:
                raise Exception(f"ERR!!!! Mismatch between articles: {art1} and {art2}")
            for i, sent in enumerate(articles_1[art1]["sentences"]):
                if len(articles_2[art2]["sentences"][i]["entities"]) > 0:
                    sent["entities"].update(
                        articles_2[art2]["sentences"][i]["entities"]
                    )
                    sent["entity_spans"].update(
                        articles_2[art2]["sentences"][i]["entity_spans"]
                    )

    return articles_1


def entity_merger(paths: list, entities: list, output_file: str):
    """
    merge same files
    """

    merged_entities = {}

    for file_, tag in zip(paths, entities):
        # Check if file is valid JSON
        if not file_.endswith(".json"):
            logging.error(f"File type error: {file_} is not a .json file")
            continue

        # Read articles
        articles = read_articles(file_)
        if articles is None:
            logging.error(f"Skipping file {file_} due to read error.")
            continue

        # Process NER parts into dictionaries
        processed_ner_article = process_articles(articles, tag)

        # Merge entities
        merged_entities = merge_two_articles(merged_entities, processed_ner_article)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(json.dumps(merged_entities, indent=2, ensure_ascii=False))

    return


def run_entity_merger(merger_config: dict):
    """
    merge all files within
    """
    paths = merger_config["paths"]
    entities = merger_config["entities"]
    output_folder = merger_config["output_path"]
    output_prefix = merger_config["output_prefix"]
    skip_existing = merger_config.get("skip_existing", False)  # New config option
    os.makedirs(output_folder, exist_ok=True)

    file_lists = {
        entity: get_sorted_files(path) for path, entity in zip(paths, entities)
    }

    # Ensure files are matched based on batch number
    for entity, files in file_lists.items():
        logging.info(f"{len(files)} files found for entity {entity}")


    # Check the START_FILE_NUMBER environment variable
    start_merge_number = int(os.getenv("START_MERGE_NBR", 0))
    stop_merge_number = int(os.getenv("STOP_MERGE_NBR", 0))
    logging.info(f"START_FILE_NUMBER: {start_merge_number}")

    if stop_merge_number > 0:
        max_files = stop_merge_number
        logging.info(f"STOP_FILE_NUMBER: {stop_merge_number}")
    else:
        max_files = max([len(files) for files in file_lists.values()])

    # Merge files by matching their batch numbers

    for i in trange(start_merge_number, max_files):
        processed_paths = []
        batch_no = None
        for entity, files in file_lists.items():
            if i < len(files):
                file = files[i]
                if batch_no is None:
                    batch_no = get_batch_no_from_filename(file)
                else:
                    if get_batch_no_from_filename(file) != batch_no:
                        logging.warning(
                            f"Batch number mismatch for entity {entity} in file {file}"
                        )
                        continue  # Skip files with mismatched batch numbers
                processed_paths.append(file)
            else:
                logging.warning(
                    f"File missing for entity {entity} at index {i}. Skipping this entity."
                )
                continue

        if len(processed_paths) == len(
            entities
        ):  # If all files are available for merging
            output_file = os.path.join(
                output_folder, output_prefix + batch_no + ".json"
            )

            # Skip merging if the output file already exists and skip_existing is True
            if skip_existing and os.path.exists(output_file):
                logging.warning(f"Skipping batch {i} as {output_file} already exists.")
                continue

            logging.info(f"Merging files: {processed_paths}")
            entity_merger(
                paths=processed_paths, entities=entities, output_file=output_file
            )
        else:
            logging.info(
                f"Skipping merging for batch {batch_no} due to missing or mismatched files."
            )

    return


def get_batch_no_from_filename(filename):
    return re.findall(r"\d+", filename)[-1]


if __name__ == "__main__":
    # Example usage
    input_folders = [
        "../../NER_pipeline/results/ner_disease/",
        "../../NER_pipeline/results/ner_phenomena/",
    ]
    infile_entity_tags = ["disease", "phenomenon"]
    merger_config = {
        "paths": input_folders,
        "entities": infile_entity_tags,
        "output_path": "./merged_results/",
        "output_prefix": "merged_",
    }

    run_entity_merger(merger_config)
