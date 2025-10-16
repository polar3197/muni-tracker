import requests
import json
import os
import boto3
import pandas as pd
from google.transit import gtfs_realtime_pb2
from datetime import datetime, timezone
import pytz
from src.database.db_manager import DatabaseManager

# The ingestor class allows fetching of vehicle data from the SFMTA MUNI GTFS
# feed, and allows writing of that data to either a local relational database,
# or to the cloud.

class MuniIngester:

    def __init__(self, database_manager):
        self.api_key = os.getenv('MUNI_API_KEY')
        self.database_manager = database_manager
        
        if not self.api_key:
            raise ValueError("no specified API key")

        self.url = f"http://api.511.org/transit/vehiclepositions?api_key={self.api_key}&agency=SF"


    def process_vehicle_data(self, entity):
        if not entity.HasField("vehicle"):
            return None
        
        v = entity.vehicle
        
        # Time conversion
        dt = datetime.fromtimestamp(v.timestamp, tz=timezone.utc)
        dt_local = dt.astimezone(pytz.timezone("America/Los_Angeles"))
        
        # Trip data
        trip = v.trip if v.HasField("trip") else None
        trip_id = getattr(trip, 'trip_id', None) if trip else None
        route_id = getattr(trip, 'route_id', None) if trip else None
        direction_id = getattr(trip, 'direction_id', None) if trip else None
        
        # Position data
        pos = v.position if v.HasField("position") else None
        lat = getattr(pos, 'latitude', None) if pos else None
        lon = getattr(pos, 'longitude', None) if pos else None
        bearing = getattr(pos, 'bearing', None) if pos and pos.HasField("bearing") else None
        speed_mph = getattr(pos, 'speed', None) if pos and pos.HasField("speed") else None
        
        # Vehicle status
        vehicle_id = getattr(v.vehicle, 'id', None) if v.HasField("vehicle") else None
        stop_id = getattr(v, 'stop_id', None) if v.HasField("stop_id") else None
        current_stop_sequence = getattr(v, 'current_stop_sequence', None) if v.HasField("current_stop_sequence") else None
        current_status = getattr(v, 'current_status', None) if v.HasField("current_status") else None
        occupancy = getattr(v, 'occupancy_status', None) if v.HasField("occupancy_status") else None
        
        return {
            'timestamp': dt_local.isoformat(),
            'active': bool(trip_id),
            'trip_id': trip_id,
            'route_id': route_id,
            'direction_id': direction_id,
            'vehicle_id': vehicle_id,
            'lat': lat,
            'lon': lon,
            'bearing': bearing,
            'speed_mph': speed_mph,
            'current_stop_sequence': current_stop_sequence,
            'current_status': current_status,
            'stop_id': stop_id,
            'occupancy': occupancy,
        }

    def fetch_vehicle_data(self):
        # poll SFMTA MUNI GTFS feed
        response = requests.get(self.url)
        response.raise_for_status()

        # Parse protocolbuf into string
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        vehicles = []
    
        # inspect and process data from each vehicle in the snapshot
        for entity in feed.entity:
            vehicle_data = self.process_vehicle_data(entity)
            if vehicle_data:
                
                # For database
                vehicles.append(vehicle_data)

        return pd.DataFrame(vehicles)

    def write_to_postgres(self):

        df = self.fetch_vehicle_data()

        if df.empty:
            print("No vehicle data to write")
            return 0
        
        # identify partition of new batch
        batch_timestamp = pd.to_datetime(df['timestamp'].iloc[0])
        week = batch_timestamp.isocalendar().week
        year = batch_timestamp.year

        self.database_manager.createNewVehiclesPartition(week, year)

        df.to_sql('vehicles', self.database_manager.engine, 
                  if_exists='append', index=False)
        
        print(f"Wrote {len(df)} vehicle records to database")
        return len(df)
