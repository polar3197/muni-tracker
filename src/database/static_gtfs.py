from sqlalchemy import create_engine, text
import pandas as pd
import os
import requests
import zipfile
import subprocess
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


class StaticDataManager:
    def __init__(self, db_url, static_data_dir, api_key):
        """Initialize the static GTFS data manager"""
        self.engine = create_engine(db_url)
        self.static_output_dir = Path(static_data_dir)
        self.data_dir = self.static_output_dir / "data"
        self.temp_zip_path = self.static_output_dir / "temp_gtfs.zip"
        self.api_key = api_key
        
        # Create directories if they don't exist
        self.static_output_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def downloadGTFSData(self, operator_id="RG"):
        """Download GTFS static data from 511 API"""
        url = f"http://api.511.org/transit/datafeeds?api_key={self.api_key}&operator_id={operator_id}"
        
        try:
            print("Downloading GTFS data from 511 API...")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            with open(self.temp_zip_path, "wb") as binary_file:
                binary_file.write(response.content)
            
            print(f"Downloaded {len(response.content)} bytes")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"Error downloading GTFS data: {e}")
            return False
    
    def extractGTFSData(self):
        """Extract GTFS zip file to data directory"""
        try:
            # Clear out old data
            if self.data_dir.exists():
                subprocess.run(f"rm -rf {self.data_dir}/*", shell=True, check=True)
                print("Cleared old GTFS data")
            
            # Extract new data
            with zipfile.ZipFile(self.temp_zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.data_dir)
            
            print(f"Extracted GTFS files to {self.data_dir}")
            
            # Clean up temp zip file
            self.temp_zip_path.unlink()
            
            return True
            
        except Exception as e:
            print(f"Error extracting GTFS data: {e}")
            return False
    
    def refreshGTFSData(self):
        """Download and extract fresh GTFS data"""
        if self.downloadGTFSData():
            if self.extractGTFSData():
                print("GTFS data refresh complete")
                return True
        print("GTFS data refresh failed")
        return False
    
    def createRoutesTable(self):
        """Create the routes table if it doesn't exist"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS routes (
                    route_id VARCHAR(50) PRIMARY KEY,
                    agency_id VARCHAR(50),
                    route_short_name VARCHAR(50),
                    route_long_name VARCHAR(255),
                    route_desc TEXT,
                    route_type INTEGER,
                    route_url VARCHAR(255),
                    route_color VARCHAR(10),
                    route_text_color VARCHAR(10),
                    route_sort_order INTEGER,
                    stops TEXT[]
                );
                
                CREATE INDEX IF NOT EXISTS routes_short_name_idx 
                ON routes(route_short_name);
            """))
            conn.commit()
    
    def createStopsTable(self):
        """Create the stops table if it doesn't exist"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS stops (
                    stop_id VARCHAR(50) PRIMARY KEY,
                    name VARCHAR(255),
                    lat DOUBLE PRECISION,
                    lon DOUBLE PRECISION
                );
                
                CREATE INDEX IF NOT EXISTS stops_location_idx 
                ON stops(lat, lon);
            """))
            conn.commit()
    
    def createTripsTable(self):
        """Create the trips table if it doesn't exist"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS trips (
                    trip_id VARCHAR(50) PRIMARY KEY,
                    route_id VARCHAR(50),
                    shape_id VARCHAR(50),
                    direction_id INTEGER,
                    FOREIGN KEY (route_id) REFERENCES routes(route_id)
                );
                
                CREATE INDEX IF NOT EXISTS trips_route_idx 
                ON trips(route_id);
            """))
            conn.commit()
    
    def createShapesTable(self):
        """Create the shapes table with PostGIS geometry"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE EXTENSION IF NOT EXISTS postgis;
                
                CREATE TABLE IF NOT EXISTS shapes (
                    shape_id VARCHAR(50) PRIMARY KEY,
                    route_line GEOMETRY(LINESTRING, 4326),
                    total_distance DOUBLE PRECISION
                );
                
                CREATE INDEX IF NOT EXISTS shapes_geom_idx 
                ON shapes USING GIST(route_line);
            """))
            conn.commit()
    
    def loadRoutesFromCSV(self):
        """Load routes data from GTFS CSV into database"""
        routes_csv = self.data_dir / "routes.txt"
        
        if not routes_csv.exists():
            print(f"Routes file not found: {routes_csv}")
            return
        
        routes_df = pd.read_csv(routes_csv)
        
        # Select relevant columns
        columns_to_load = ['route_id', 'agency_id', 'route_short_name', 'route_long_name',
                          'route_desc', 'route_type', 'route_url', 'route_color',
                          'route_text_color', 'route_sort_order']
        
        routes_clean = routes_df[columns_to_load].copy()
        
        # Truncate table instead of dropping (avoids foreign key issues)
        with self.engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE routes CASCADE"))
            conn.commit()
        
        # Bulk insert
        routes_clean.to_sql(
            'routes',
            self.engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=100
        )
        
        print(f"Successfully loaded {len(routes_clean)} routes")
    
    def updateRoutesWithStops(self):
        """Update routes table with aggregated stops list from vehicles"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                UPDATE routes
                SET stops = subq.stops_list
                FROM (
                    SELECT 
                        route_id,
                        ARRAY_AGG(DISTINCT stop_id) AS stops_list
                    FROM vehicles
                    WHERE stop_id IS NOT NULL
                    GROUP BY route_id
                    HAVING COUNT(*) > 5
                    AND COUNT(DISTINCT vehicle_id) > 1
                ) AS subq
                WHERE routes.route_id = subq.route_id
            """))
            conn.commit()
            print("Successfully updated routes with stops")
    
    def loadStopsFromCSV(self):
        """Load stops data from GTFS CSV into database"""
        stops_csv = self.data_dir / "stops.txt"
        
        if not stops_csv.exists():
            print(f"Stops file not found: {stops_csv}")
            return
        
        stops_df = pd.read_csv(stops_csv)
        
        # Get stops that actually exist in vehicle data
        with self.engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT(stop_id)
                FROM vehicles
                WHERE stop_id IS NOT NULL
            """))
            muni_stops = {str(row[0]) for row in result.fetchall()}
        
        if not muni_stops:
            print("No vehicle data found - loading all stops")
            filtered_stops = stops_df
        else:
            # Filter to only MUNI stops that have vehicle data
            filtered_stops = stops_df[stops_df['stop_id'].astype(str).isin(muni_stops)]
        
        # Clean and prepare data
        filtered_stops_clean = filtered_stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']].copy()
        filtered_stops_clean.columns = ['stop_id', 'name', 'lat', 'lon']
        
        # Truncate table first
        with self.engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE stops CASCADE"))
            conn.commit()
        
        # Bulk insert
        filtered_stops_clean.to_sql(
            'stops', 
            self.engine, 
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"Successfully loaded {len(filtered_stops_clean)} stops")
    
    def loadTripsFromCSV(self):
        """Load trips data from GTFS CSV into database"""
        trips_csv = self.data_dir / "trips.txt"
        
        if not trips_csv.exists():
            print(f"Trips file not found: {trips_csv}")
            return
        
        trips_df = pd.read_csv(trips_csv)
        
        # Filter to SF trips only
        trips_filtered = trips_df[trips_df['trip_id'].str.startswith('SF')][
            ['trip_id', 'route_id', 'shape_id', 'direction_id']
        ].copy()
        
        # Truncate table first
        with self.engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE trips CASCADE"))
            conn.commit()
        
        # Bulk insert
        trips_filtered.to_sql(
            'trips',
            self.engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"Successfully loaded {len(trips_filtered)} trips")
    
    def loadShapesFromCSV(self):
        """Load shape/route geometry data from GTFS CSV into database"""
        shapes_csv = self.data_dir / "shapes.txt"
        
        if not shapes_csv.exists():
            print(f"Shapes file not found: {shapes_csv}")
            return
        
        shapes_df = pd.read_csv(shapes_csv)
        
        # Filter to SF shapes only
        shapes_df = shapes_df[shapes_df['shape_id'].str.startswith('SF')]
        
        # Clear existing shapes
        with self.engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE shapes"))
            conn.commit()
        
        print(f"Loading {len(shapes_df['shape_id'].unique())} shapes...")
        
        with self.engine.connect() as conn:
            for i, shape_id in enumerate(shapes_df['shape_id'].unique()):
                # Get all points for this shape
                shape_points = shapes_df[shapes_df['shape_id'] == shape_id].sort_values('shape_pt_sequence')
                
                # Calculate total distance and convert to Python float
                total_distance = float(shape_points['shape_dist_traveled'].max())
                
                # Build LINESTRING from points
                linestring = f"LINESTRING({', '.join([f'{row.shape_pt_lon} {row.shape_pt_lat}' for row in shape_points.itertuples()])})"
                
                # Insert shape
                conn.execute(text("""
                    INSERT INTO shapes (shape_id, route_line, total_distance)
                    VALUES (:shape_id, ST_GeomFromText(:linestring, 4326), :total_distance)
                """), {
                    'shape_id': shape_id,
                    'linestring': linestring,
                    'total_distance': total_distance
                })
                
                if (i + 1) % 100 == 0:
                    print(f"  Loaded {i + 1} shapes...")
            
            conn.commit()
        
        print(f"Successfully loaded {len(shapes_df['shape_id'].unique())} shapes")

    def createStopTimesTable(self):
        """Create the stop_times table if it doesn't exist"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS stop_times (
                    trip_id VARCHAR(50),
                    stop_id VARCHAR(50),
                    stop_sequence INTEGER,
                    arrival_time VARCHAR(8),
                    departure_time VARCHAR(8),
                    PRIMARY KEY (trip_id, stop_sequence),
                    FOREIGN KEY (trip_id) REFERENCES trips(trip_id),
                    FOREIGN KEY (stop_id) REFERENCES stops(stop_id)
                );
                
                CREATE INDEX IF NOT EXISTS stop_times_trip_idx 
                ON stop_times(trip_id);
                
                CREATE INDEX IF NOT EXISTS stop_times_stop_idx 
                ON stop_times(stop_id);
            """))
            conn.commit()

    def loadStopTimesFromCSV(self):
        """Load stop times data from GTFS CSV into database"""
        stop_times_csv = self.data_dir / "stop_times.txt"
        
        if not stop_times_csv.exists():
            print(f"Stop times file not found: {stop_times_csv}")
            return
        
        print("Loading stop times (this may take a while)...")
        
        # Read in chunks because stop_times is usually huge
        chunk_size = 10000
        chunks_processed = 0
        
        # Truncate table first
        with self.engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE stop_times CASCADE"))
            conn.commit()
        
        for chunk in pd.read_csv(stop_times_csv, chunksize=chunk_size):
            # Filter to SF trips only
            sf_chunk = chunk[chunk['trip_id'].str.startswith('SF')][
                ['trip_id', 'stop_id', 'stop_sequence', 'arrival_time', 'departure_time']
            ].copy()
            
            if len(sf_chunk) > 0:
                sf_chunk.to_sql(
                    'stop_times',
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                chunks_processed += 1
                print(f"  Processed {chunks_processed * chunk_size} rows...")
        
        print(f"Successfully loaded stop times")
    
   def initializeAllTables(self):
        """Create all necessary tables for static GTFS data"""
        print("Creating tables...")
        self.createRoutesTable()
        self.createStopsTable()
        self.createTripsTable()
        self.createStopTimesTable()  # Add this
        self.createShapesTable()
        print("All tables created successfully")

    def loadAllStaticData(self):
        """Load all static GTFS data from CSV files"""
        print("Loading static GTFS data...")
        self.loadRoutesFromCSV()
        self.loadStopsFromCSV()
        self.loadTripsFromCSV()
        self.loadStopTimesFromCSV()  # Add this (warning: can be slow/large)
        self.loadShapesFromCSV()
        
        print("All static data loaded successfully") 

    def fullRefresh(self):
        """Complete refresh: download, extract, and load GTFS data"""
        print("=== Starting Full GTFS Data Refresh ===")
        
        # Step 1: Download and extract
        if not self.refreshGTFSData():
            print("Failed to download/extract GTFS data")
            return False
        
        # Step 2: Initialize tables
        self.initializeAllTables()
        
        # Step 3: Load all data
        self.loadAllStaticData()
        
        print("=== Full GTFS Data Refresh Complete ===")
        return True


# Usage example
def main():
    # Get environment variables
    db_name = os.environ.get("POSTGRES_DB") 
    db_user = os.environ.get("POSTGRES_USERNAME")
    db_password = os.environ.get("POSTGRES_PASSWORD", "")
    db_host = os.environ.get("POSTGRES_HOST", "localhost")  # Uses POSTGRES_HOST
    db_port = os.environ.get("POSTGRES_PORT", "5432")
    static_data_dir = os.environ.get("STATIC_MUNI_DATA")
    api_key = os.environ.get("MUNI_API_KEY")
    
    if not api_key:
        raise ValueError("MUNI_API_KEY environment variable not set")
    
    # Build database URL
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    # Initialize manager
    manager = StaticDataManager(db_url, static_data_dir, api_key)
    
    # Do a full refresh (download + load)
    manager.fullRefresh()


if __name__ == "__main__":
    main()
