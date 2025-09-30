from muni_ingester import MuniIngester
from partition_manager import PartitionManager
import os


POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
S3_BUCKET = os.getenv("S3_BUCKET")

def main():
    db_url = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{os.getenv('POSTGRES_HOST', 'localhost')}:5432/{POSTGRES_DB}"

    pm = PartitionManager(
        db_url=db_url,
        s3_bucket=S3_BUCKET
    )

    ingester = MuniIngester(partition_manager=pm)

    try:
        count = ingester.write_to_postgres()
        print(f"Wrote {count} records")
    except Exception as e:
        print(f"Error: {e}")
