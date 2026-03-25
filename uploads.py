import pandas as pd
import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.client import Config

load_dotenv()

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



def upload_paquet_file_to_cellar(file_path):
    """
    Upload a paquet file to cellar
    """
    endpoint = os.getenv('CELLAR_ENDPOINT')
    access_key = os.getenv('CELLAR_ACCESS_KEY')
    secret_key = os.getenv('CELLAR_SECRET_KEY')
    bucket_name = os.getenv('CELLAR_BUCKET_NAME')

    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="default",
        config=Config(
            signature_version='s3v4',
            request_checksum_calculation='WHEN_REQUIRED',
            response_checksum_validation='WHEN_REQUIRED'
        )
    )

    try:
        print(f"Uploading {file_path} to {bucket_name}...")
        s3.upload_file(file_path, bucket_name, os.path.basename(file_path))
        print(f"Upload complete !")
    except FileNotFoundError:
        print(f"ERROR : file {file_path} not found.")
    except NoCredentialsError:
        print("ERROR : invalid credentials.")
    except Exception as e:
        print(f"ERROR : {e}")

file1 = "municipales-2026-resultats-bv-par-communes-2026-03-20"
file2 = "municipales-2026-resultats-bureau-de-vote-2026-03-23-16h15"

convert_csv_to_parquet(f"./data/{file1}.csv", f"./output/{file1}", ";")
convert_csv_to_parquet(f"./data/{file2}.csv", f"./output/{file2}", ";")

upload_paquet_file_to_cellar(f"./output/{file1}.parquet")
upload_paquet_file_to_cellar(f"./output/{file2}.parquet")

os.remove(f"./output/{file1}.parquet")
os.remove(f"./output/{file2}.parquet")
