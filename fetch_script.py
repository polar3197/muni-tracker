from muni_ingester import MuniIngester
from db_manager import DatabaseManager
import os


POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
S3_BUCKET = os.getenv("S3_BUCKET")

def main():
    db_url = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"

    dm = DatabaseManager(
        db_url=db_url,
        s3_bucket=S3_BUCKET
    )

    ingester = MuniIngester(database_manager=dm)

    try:
        count = ingester.write_to_postgres()
        print(f"Wrote {count} records")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
