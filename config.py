import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
con = duckdb.connect(database=':memory:')

bucket = os.getenv('CELLAR_BUCKET_NAME')
files = {
    1: f"s3://{bucket}/municipales-2026-resultats-bv-par-communes-2026-03-20.parquet",
    2: f"s3://{bucket}/municipales-2026-resultats-bureau-de-vote-2026-03-23-16h15.parquet"
}