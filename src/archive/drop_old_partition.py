from src.database.db_manager import DatabaseManager
from sqlalchemy import create_engine

def main():
    # this will be run once a week. Because of this, it should 

    # 1. connect to database
    dbm = DatabaseManager()

    dbm.exportOldestPartitionTos3()
    
    # 1. query to find oldest parition


    # 2. insert that into an sql export query

    # 3. send the data over boto3 to the appropriate s3 bucker

    # 4. drop the partition

    return

if __name__ == "__main__":
    main()
