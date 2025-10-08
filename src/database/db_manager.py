from sqlalchemy import create_engine, text
import boto3
import pandas as pd
import re
from datetime import datetime, timedelta


class DatabaseManager():
    def __init__(self, db_url, s3_bucket):
        # establish connection point with pg db
        self.engine = create_engine(db_url)
        self.s3 = boto3.client('s3')
        self.bucket = s3_bucket
    
    # This function allows scheduling of the job so that each week a 
    # new partition can be made and the oldest one can be written to s3
    def exportOldestPartitionTos3(self):

        # identify year/week of current moment
        four_weeks_ago = datetime.now() - timedelta(weeks=4)
        year, week_of_year, dow = four_weeks_ago.isocalendar()

        # query to find any partitions older than four weeks
        query = f"""SELECT tablename FROM pg_tables 
                    WHERE tablename LIKE 'vehicles_partition_%_w%' AND 
                          tablename < 'vehicles_partition_{year}_w{week_of_year}'
                """

        with self.engine.connect as conn:
            partitions_to_be_exported = conn.execute(text(query))
            conn.commit()
            
        # iterate through partition names that are > 4 weeks old and still in db
        for p in partitions_to_be_exported:
            match = re.match("vehicles_partition_(\d+)_w(\d+)", p)

            # extract week and year from partition name    
            year = match.group(1)
            week = match.group(2)

            # read partition from postgres
            query = f"SELECT * FROM vehicles_partition_{year}_w{week}"
            df = pd.read_sql(query, self.engine)

            # write to parquet
            parquet = df.to_parquet(compression="snappy")

            # upload to s3
            s3_key = f"vehicle_records/{year}/{week}.parquet"
            self.s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=parquet
            )
            print(f"Successfully exported vehicles_partition_{year}_w{week} to S3")

        return len(partitions_to_be_exported)
        
    
    def createNewVehiclesPartition(self, week, year): 
        partition_name = f"vehicles_partition_{year}_w{week}"

        start_of_week = datetime.datetime(year, 1, 1) + datetime.timedelta(weeks=week - 1)
        end_of_week = start_of_week + datetime.timedelta(weeks=1)

        with self.engine.connect() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {partition_name} 
                PARTITION OF vehicles
                FOR VALUES FROM ('{start_of_week}') TO ('{end_of_week}');
                              
                CREATE INDEX IF NOT EXISTS {partition_name}_route_idx
                ON {partition_name}(route_id);

                CREATE INDEX IF NOT EXISTS {partition_name}_time_idx
                ON {partition_name}(timestamp);
            """))
            conn.commit()
    
    def createVehiclesTable(self):
        with self.engine.connect() as conn:  # Add ()
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS vehicles (
                    id SERIAL,
                    vehicle_id VARCHAR(50) NOT NULL,
                    route_id VARCHAR(50),
                    lat DOUBLE PRECISION,
                    lon DOUBLE PRECISION,
                    bearing DOUBLE PRECISION,
                    speed_mph DOUBLE PRECISION,
                    timestamp TIMESTAMP NOT NULL,
                    PRIMARY KEY (id, timestamp)
                ) PARTITION BY RANGE (timestamp);
            """))
            conn.commit()



