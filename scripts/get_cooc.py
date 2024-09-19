from glob import glob
import pandas as pd
import os


from co_occurence import get_pairs, create_df_from_pairs

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

input_dir = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/merged/"
results_dir = os.path.join(script_dir, "results")

sorted_files = sorted(
    glob(f"{input_dir}*.json"), key=lambda f: int("".join(filter(str.isdigit, f)))
)

for i in sorted_files[:20]:
    print(i)


pairs = get_pairs(sorted_files, entity1="disease", entity2="phenoma")


def dict_to_dataframe(pairs_dict):
    """
    Convert a dictionary of co-occurrence pairs into a pandas DataFrame.

    Parameters:
        pairs_dict (dict): The dictionary containing co-occurrence pairs.

    Returns:
        pd.DataFrame: DataFrame containing the co-occurrence pairs.
    """
    data = []

    # Loop through the dictionary and flatten it into rows
    for e1, subdict in pairs_dict.items():
        for e2, details in subdict.items():
            row = {
                "entity_1": e1,
                "entity_2": e2,
                "frequency": details["freq"],
                "pmid": ", ".join(
                    details["pmid"]
                ),  # Convert set of PMIDs to comma-separated string
                "sentences": " | ".join(
                    details["sent"]
                ),  # Convert set of sentences to pipe-separated string
            }
            data.append(row)

    # Create a DataFrame from the list of rows
    df = pd.DataFrame(data)
    return df


def save_dataframe_to_csv(df, output_file):
    """
    Save the DataFrame to a CSV file.

    Parameters:
        df (pd.DataFrame): The DataFrame containing entity co-occurrences.
        output_file (str): The path to the CSV file.
    """
    df.to_csv(output_file, index=False)


# df = dict_to_dataframe(pairs)
# save_dataframe_to_csv(df, "results/co_occurrences.csv")

save_dataframe_to_csv(create_df_from_pairs(pairs), results_dir)
