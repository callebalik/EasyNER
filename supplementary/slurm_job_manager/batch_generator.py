import json
import argparse


def split_articles_into_batches(total_articles, batch_size):
    """
    Splits the total number of articles into batches, each with a specified number of articles.
    :param total_articles: Total number of articles
    :param batch_size: Number of articles per batch
    :return: List of tuples with (start, end) indices for each batch
    """
    batches = []
    start = 1  # Start from 1 (inclusive)

    while start <= total_articles:
        end = min(start + batch_size - 1, total_articles)
        batches.append((start, end))
        start = end + 1  # Move start to the next article after the current batch

    return batches


def save_batches_to_file(batches, output_file):
    """
    Saves the list of batch intervals to a file in JSON format.
    :param batches: List of batch intervals
    :param output_file: File to save the batch intervals
    """
    with open(output_file, "w") as f:
        json.dump(batches, f, indent=2)
    print(f"{len(batches)} Batches saved to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate batch intervals for articles"
    )
    parser.add_argument(
        "--total-articles", type=int, required=True, help="Total number of articles"
    )
    parser.add_argument(
        "--batch-size", type=int, required=True, help="Number of articles per batch"
    )
    parser.add_argument(
        "--output-file",
        type=str,
        help="File to save the batch intervals (JSON format)",
        default="batches.json",
    )

    args = parser.parse_args()

    # Generate batch intervals
    batches = split_articles_into_batches(args.total_articles, args.batch_size)

    # Save batches to the output file
    save_batches_to_file(batches, args.output_file)


if __name__ == "__main__":
    main()
