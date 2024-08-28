import json
import re
import pandas as pd
from tqdm import tqdm


def load_json(input_file):
    """
    Load a JSON file and return its content as a dictionary.

    Parameters:
        input_file (str): The path to the JSON file.

    Returns:
        dict: The loaded JSON data.
    """
    with open(input_file, "r", encoding="utf-8") as file:
        return json.load(file)


def get_batch_index(input_file):
    """
    Extract a numeric batch index from a file name using regex.

    Parameters:
        input_file (str): The name of the file.

    Returns:
        int: The numeric batch index if found, else None.
    """
    match = re.search(r"\d+$", input_file)
    return int(match.group()) if match else None


def get_pairs(sorted_files, entity1, entity2):
    """
    Find co-occurrences between two different types of entities across a list of JSON files.

    Parameters:
        sorted_files (list of str): List of file paths to the JSON files.
        entity1 (str): The first entity type (e.g., "disease") to find co-occurrences for.
        entity2 (str): The second entity type (e.g., "chemical") to find co-occurrences for.

    Returns:
        dict: A nested dictionary where entity pairs are stored along with frequency, PMID, and sentences.
    """
    d = {}

    for input_file in tqdm(sorted_files, desc="Processing files"):
        batch = load_json(input_file=input_file)
        batch_idx = get_batch_index(input_file=input_file)

        for idx in batch:
            article = batch[idx]

            for s_idx, sent in enumerate(article["sentences"]):
                if len(sent["entities"]) >= 2:
                    if entity1 in sent["entities"] and entity2 in sent["entities"]:
                        for e1 in sent["entities"][entity1]:
                            if e1 not in d:
                                d[e1] = {}

                            for e2 in sent["entities"][entity2]:
                                if e2 not in d[e1]:
                                    d[e1][e2] = {
                                        "freq": 0,
                                        "pmid": set(),
                                        "sent": set(),
                                    }

                                d[e1][e2]["freq"] += 1
                                d[e1][e2]["pmid"].add(idx)
                                d[e1][e2]["sent"].add(sent["text"])

    return d


def get_self_pairs(sorted_files, entity):
    """
    Find co-occurrences within the same entity type across a list of JSON files.

    Parameters:
        sorted_files (list of str): List of file paths to the JSON files.
        entity (str): The entity type (e.g., "disease") to find co-occurrences for.

    Returns:
        dict: A dictionary where entity pairs are stored along with frequency and PMID.
    """
    d = {}

    for input_file in tqdm(sorted_files, desc="Processing files"):
        batch = load_json(input_file=input_file)
        batch_idx = get_batch_index(input_file=input_file)

        for idx in tqdm(batch, desc=f"batch:{batch_idx}"):
            article = batch[idx]

            for s_idx, sent in enumerate(article["sentences"]):
                if entity in sent["entities"]:
                    if len(sent["entities"][entity]) >= 2:
                        for i in range(len(sent["entities"][entity])):
                            for j in range(i + 1, len(sent["entities"][entity])):
                                e1 = sent["entities"][entity][i]
                                e2 = sent["entities"][entity][j]

                                if e1 != e2 and len(e1) > 1 and len(e2) > 1:
                                    if (e1, e2) in d:
                                        d[(e1, e2)]["freq"] += 1
                                        d[(e1, e2)]["pmid"].add(idx)
                                    elif (e2, e1) in d:
                                        d[(e2, e1)]["freq"] += 1
                                        d[(e2, e1)]["pmid"].add(idx)
                                    else:
                                        d[(e1, e2)] = {"freq": 1, "pmid": set([idx])}

    return d


def create_df_from_pairs(pairs_dict):
    """
    Create a pandas DataFrame from the entity pairs dictionary.

    Parameters:
        pairs_dict (dict): A dictionary where keys are tuples (entity1, entity2) and values are dicts with frequency, PMIDs, and sentences.

    Returns:
        pd.DataFrame: DataFrame containing entity pairs, frequency, PMIDs, and sentences.
    """
    data = []

    for (e1, e2), val in tqdm(pairs_dict.items(), desc="Creating DataFrame"):
        # Append data for each entity pair
        data.append([
            e1,  # First entity
            e2,  # Second entity
            val["freq"],  # Frequency
            ",".join(val["pmid"]),  # PMIDs as a comma-separated string
            "; ".join(val["sent"])  # Sentences as a semicolon-separated string
        ])

    # Create DataFrame from the data list
    return pd.DataFrame(
        data, columns=["entity_1", "


def save_pairs_to_csv(df, output_file):
    """
    Save the co-occurrence DataFrame to a CSV file.

    Parameters:
        df (pd.DataFrame): The DataFrame containing entity co-occurrences.
        output_file (str): The path to the CSV file.
    """
    df.to_csv(output_file, index=False)


def main():
    """
    Placeholder for the main method, where the script logic will be executed.
    """
    pass  # To be implemented later


if __name__ == "__main__":
    main()
