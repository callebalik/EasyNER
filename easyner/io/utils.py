import os
import re


def extract_batch_index(batch_file: str) -> int:
    """
    Extract the batch index from a filename.

    Parameters:
    -----------
    batch_file: str
        Path to the batch file

    Returns:
    --------
    int: The extracted batch index

    Raises:
    -------
    ValueError: If the filename doesn't contain a numeric index
    """
    regex = re.compile(r"\d+")
    try:
        return int(regex.findall(os.path.basename(batch_file))[-1])
    except (IndexError, ValueError) as e:
        print(f"Error extracting index from {batch_file}")
        raise ValueError(f"Batch filenames must contain numeric indices: {e}")
