from src.database.db_manager import DatabaseManager

def main():
    dbm = DatabaseManager()
    dbm.exportOldestPartitionTos3

if __name__ = "__main__":
    main()

