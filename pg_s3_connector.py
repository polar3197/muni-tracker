from sqlalchemy import create_engine, text
import boto3
import pandas as pd


class PartitionManager():
    def __init__(self, db_url, s3_bucket):
        # establish connection point with pg db
        self.engine = create_engine(db_url)
        self.s3 = boto3.client('s3')
        self.bucket = s3_bucket
    
    def exportTos3(self, partition_name):
        # read partition from postgres
        query = f"SELECT * FROM {partition_name}"
        df = pd.read_sql(query, self.engine)

        # write to parquet
        parquet = df.to_parquet(compression="snappy")

        # upload to s3
        s3_key = f"vehicle_records/{partition_name}.parquet"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=parquet
        )

        return len(df)
    
    def create_vehicles_table(self):
        with self.engine.connect as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS vehicles (
                    id SERIAL,
                    vehicle_id VARCHAR(50) NOT NULL,
                    route_id VARCHAR(50),
                    lat DOUBLE PRECISION,
                    lon DOUBLE PRECISION,
                    heading DOUBLE PRECISION,
                    speed DOUBLE PRECISION,
                    timestamp TIMESTAMP NOT NULL,
                    PRIMARY KEY (id, timestamp)
                ) PARTITION BY RANGE (timestamp);
            """))
            conn.commit()



