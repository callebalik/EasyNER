import pandas as pd

def compare_total_count(tsv_file, csv_file, output_file):
    # Read the TSV and CSV files
    tsv_data = pd.read_csv(tsv_file, sep='\t')
    csv_data = pd.read_csv(csv_file)

    # Ensure the total_count columns are numeric
    tsv_data['total_count'] = pd.to_numeric(tsv_data['total_count'], errors='coerce').fillna(0)
    csv_data['total_count'] = pd.to_numeric(csv_data['total_count'], errors='coerce').fillna(0)

    # Merge the data on the first column of TSV and entity_text of CSV
    merged_data = pd.merge(tsv_data, csv_data, left_on=tsv_data.columns[0], right_on='entity_text', how='outer', suffixes=('_json', '_database'), indicator=True)

    # Calculate the total_count difference
    merged_data['total_count_diff'] = merged_data['total_count_json'] - merged_data['total_count_database']

    # Filter the differences where total_count values differ
    diff_data = merged_data[(merged_data['_merge'] != 'both') | (merged_data['total_count_diff'] != 0)]

    # Select relevant columns for output
    output_data = diff_data[['entity_text', 'total_count_json', 'total_count_database', 'total_count_diff']]

    # Export the differences to a new file
    output_data.to_csv(output_file, index=False)

if __name__ == "__main__":
    tsv_file = '/home/x_caoll/EasyNer/results/analysis/analysis_phenoma/result_phenoma.tsv'
    csv_file = '/home/x_caoll/EasyNer/results/analysis/analysis_phenoma/entity_fq.csv'
    output_file = '/home/x_caoll/EasyNer/results/analysis/analysis_phenoma/diff_file.csv'

    compare_total_count(tsv_file, csv_file, output_file)
