from src.database.db_manager import DatabaseManager

def main():
    # this will be run once a week. Because of this, it should 
    
    # 1. query to find oldest parition


    # 2. insert that into an sql export query

    # 3. send the data over boto3 to the appropriate s3 bucker

    # 4. drop the partition

    print(f"Successfully exported partition {year}-{week} to S3")

    return

if __name__ == "__main__":
    main()
