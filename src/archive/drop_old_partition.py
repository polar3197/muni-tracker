from src.database.db_manager import DatabaseManager
from sqlalchemy import create_engine
import os

def main():
    postgres_pw = os.getenv("POSTGRES_PASSWORD")
    postgres_un = os.getenv("POSTGRES_USERNAME")
    postgres_db = os.getenv("POSTGRES_DB")
    postgres_host = os.getenv("POSTGRES_HOST")
    db_url = f"postgresql://{postgres_un}:{postgres_pw}@{postgres_host}:5432/{postgres_db}"

    s3_bucket = os.getenv("charlie-muni-pg-backup")
    # this will be run once a week. Because of this, it should 

    # 1. connect to database
    dbm = DatabaseManager(db_url, s3_bucket)

    dbm.exportOldestPartitionTos3()
    
    # 1. query to find oldest parition


    # 2. insert that into an sql export query

    # 3. send the data over boto3 to the appropriate s3 bucker

    # 4. drop the partition

    return

if __name__ == "__main__":
    main()
