import pandas as pd

def convert_csv_to_parquet(file_path, output_name, separator=","):
    """
    Convert a csv file to .parquet format.
    Some columns types are ambiguous, so we force them to be a certain type
    """
    forced_types = {
        'Code département': str,
        'Code commune': str,
        'Code bureau de vote': str
    }

    # Import csv file
    file_data = pd.read_csv(
            file_path,
            sep=separator,
            dtype=forced_types,
            low_memory=False
        )

    # convert the file
    file_data.to_parquet(output_name + '.parquet', engine='pyarrow')

convert_csv_to_parquet("./data/municipales-2026-resultats-bureau-de-vote-2026-03-23-16h15.csv", "./output/municipales-2026-resultats-bureau-de-vote-2026-03-23-16h15", ";")


