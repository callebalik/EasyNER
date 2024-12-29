import os
import re

def rename_merged_files(directory):
    """
    Rename up to `limit` merged files in the specified directory to include a hyphen between the prefix and the numeric part.
    """
    for filename in os.listdir(directory):
        if filename.startswith("merged") and filename.endswith(".json"):
            # Extract the numeric part from the filename
            match = re.search(r'(\d+)(?!.*\d)', filename)
            if match:
                numeric_part = match.group(0)
                # Construct the new filename with a hyphen
                new_filename = f"merged-{numeric_part}.json"
                old_filepath = os.path.join(directory, filename)
                new_filepath = os.path.join(directory, new_filename)
                # Rename the file
                os.rename(old_filepath, new_filepath)
                print(f"Renamed: {filename} -> {new_filename}")
            else:
                print(f"No numeric part found in filename: {filename}")

# Specify the directory containing the merged files
directory = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/ner_merged"

# Run the rename function with a limit of 10 files
rename_merged_files(directory)