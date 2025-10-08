from src.database.db_manager import DatabaseManager
from sqlalchemy import create_engine

def main():
    db_url = f"postgresql://{os.getenv("POSTGRES_USERNAME")}:{os.getenv("POSTGRES_PASSWORD")}@{os.getenv("POSTGRES_HOST")}:5432/{os.getenv("POSTGRES_DB")}"

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
