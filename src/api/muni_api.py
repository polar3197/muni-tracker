from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from typing import Optional
import os


# Database connection
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

app = FastAPI(title="MUNI Tracker API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "MUNI API is running", "version": "1.0.0"}

@app.get("/health")
async def health():
    """Health check endpoint"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.get("/vehicles/current")
async def get_current_vehicles(route_id: Optional[str] = None):
    """Get the latest position for each vehicle, optionally filtered by route"""
    with engine.connect() as conn:
        if route_id:
            query = text("""
                SELECT 
                    v.timestamp, 
                    v.vehicle_id, 
                    v.route_id, 
                    v.lat, 
                    v.lon, 
                    v.bearing, 
                    v.speed_mph, 
                    v.active, 
                    v.trip_id, 
                    v.occupancy,
                    v.stop_id as next_stop_id,
                    s.name as next_stop_name,
                    r.route_short_name,
                    r.route_long_name,
                    r.route_color,
                    r.route_type
                FROM vehicles v
                LEFT JOIN stops s ON v.stop_id::VARCHAR = s.stop_id
                LEFT JOIN routes r ON v.route_id = r.route_short_name
                WHERE v.timestamp = (SELECT MAX(timestamp) FROM vehicles)
                AND (v.route_id = :route_id OR r.route_short_name = :route_id)
            """)
            result = conn.execute(query, {"route_id": route_id})
        else:
            query = text("""
                SELECT 
                    v.timestamp, 
                    v.vehicle_id, 
                    v.route_id, 
                    v.lat, 
                    v.lon, 
                    v.bearing, 
                    v.speed_mph, 
                    v.active, 
                    v.trip_id, 
                    v.occupancy,
                    v.stop_id as next_stop_id,
                    s.name as next_stop_name,
                    r.route_short_name,
                    r.route_long_name,
                    r.route_color,
                    r.route_type
                FROM vehicles v
                LEFT JOIN stops s ON v.stop_id::VARCHAR = s.stop_id
                LEFT JOIN routes r ON v.route_id = r.route_short_name
                WHERE v.timestamp = (SELECT MAX(timestamp) FROM vehicles)
            """)
            result = conn.execute(query)
        
        return [dict(row._mapping) for row in result]

@app.get("/vehicles/{vehicle_id}")
async def get_vehicle_details(vehicle_id: str):
    """Get detailed information about a specific vehicle"""
    with engine.connect() as conn:
        query = text("""
            SELECT 
                v.timestamp,
                v.vehicle_id,
                v.route_id,
                v.lat,
                v.lon,
                v.bearing,
                v.speed_mph,
                v.active,
                v.trip_id,
                v.occupancy,
                v.stop_id as next_stop_id,
                s.name as next_stop_name,
                s.lat as next_stop_lat,
                s.lon as next_stop_lon,
                r.route_short_name,
                r.route_long_name,
                r.route_color,
                r.route_type
            FROM vehicles v
            LEFT JOIN stops s ON v.stop_id::VARCHAR = s.stop_id
            LEFT JOIN routes r ON v.route_id = r.route_id
            WHERE v.vehicle_id = :vehicle_id
            ORDER BY v.timestamp DESC
            LIMIT 1
        """)
        result = conn.execute(query, {"vehicle_id": vehicle_id})
        row = result.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Vehicle not found")
        
        return dict(row._mapping)

@app.get("/routes")
async def get_all_routes():
    """Get list of all active routes"""
    with engine.connect() as conn:
        query = text("""
            SELECT 
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_type,
                r.route_color,
                r.route_text_color,
                COUNT(DISTINCT v.vehicle_id) as active_vehicles
            FROM routes r
            LEFT JOIN vehicles v ON r.route_id = v.route_id 
                AND v.timestamp = (SELECT MAX(timestamp) FROM vehicles)
            GROUP BY r.route_id, r.route_short_name, r.route_long_name, 
                     r.route_type, r.route_color, r.route_text_color
            HAVING COUNT(DISTINCT v.vehicle_id) > 0
            ORDER BY r.route_short_name
        """)
        result = conn.execute(query)
        
        routes = []
        for row in result:
            route = dict(row._mapping)
            # Add route type name
            route_types = {
                0: "Tram/Light Rail",
                1: "Subway/Metro",
                2: "Rail",
                3: "Bus",
                4: "Ferry",
                5: "Cable Car",
                6: "Gondola",
                7: "Funicular"
            }
            route['route_type_name'] = route_types.get(route['route_type'], 'Unknown')
            routes.append(route)
        
        return routes

@app.get("/routes/{route_id}")
async def get_route_details(route_id: str):
    """Get detailed information about a specific route"""
    with engine.connect() as conn:
        # Get route info - match by route_id OR route_short_name
        route_query = text("""
            SELECT 
                r.route_id,
                r.route_short_name,
                r.route_long_name,
                r.route_desc,
                r.route_type,
                r.route_color,
                r.route_text_color,
                COUNT(DISTINCT v.vehicle_id) as active_vehicles
            FROM routes r
            LEFT JOIN vehicles v ON r.route_id = v.route_id 
                AND v.timestamp = (SELECT MAX(timestamp) FROM vehicles)
            WHERE r.route_id = :route_id OR r.route_short_name = :route_id
            GROUP BY r.route_id, r.route_short_name, r.route_long_name, 
                     r.route_desc, r.route_type, r.route_color, 
                     r.route_text_color
        """)
        result = conn.execute(route_query, {"route_id": route_id})
        row = result.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Route not found")
        
        route = dict(row._mapping)
        
        # Add route type name
        route_types = {
            0: "Tram/Light Rail",
            1: "Subway/Metro",
            2: "Rail",
            3: "Bus",
            4: "Ferry",
            5: "Cable Car",
            6: "Gondola",
            7: "Funicular"
        }
        route['route_type_name'] = route_types.get(route['route_type'], 'Unknown')
        
        return route

@app.get("/routes/{route_id}/stops")
async def get_route_stops(route_id: str):
    """Get all stops for a specific route using proper GTFS relationships"""
    with engine.connect() as conn:
        query = text("""
            SELECT DISTINCT
                s.stop_id,
                s.name,
                s.lat,
                s.lon
            FROM routes r
            JOIN trips t ON r.route_id = t.route_id
            JOIN stop_times st ON t.trip_id = st.trip_id
            JOIN stops s ON st.stop_id = s.stop_id
            WHERE r.route_id = :route_id OR r.route_short_name = :route_id
            ORDER BY s.name
        """)
        result = conn.execute(query, {"route_id": route_id})
        
        return [dict(row._mapping) for row in result]

@app.get("/stops/{stop_id}")
async def get_stop_details(stop_id: str):
    """Get detailed information about a specific stop"""
    with engine.connect() as conn:
        query = text("""
            SELECT 
                s.stop_id,
                s.name,
                s.lat,
                s.lon,
                COUNT(DISTINCT v.vehicle_id) as vehicles_approaching
            FROM stops s
            LEFT JOIN vehicles v ON s.stop_id = v.stop_id::VARCHAR
                AND v.timestamp = (SELECT MAX(timestamp) FROM vehicles)
            WHERE s.stop_id = :stop_id
            GROUP BY s.stop_id, s.name, s.lat, s.lon
        """)
        result = conn.execute(query, {"stop_id": stop_id})
        row = result.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Stop not found")
        
        return dict(row._mapping)

@app.get("/stops")
async def get_stops(route_ids: Optional[str] = None):
    """Get stops, optionally filtered by route IDs or route short names (comma-separated)"""
    with engine.connect() as conn:
        if route_ids:
            # Convert comma-separated string to list
            route_id_list = [rid.strip() for rid in route_ids.split(',')]
            
            query = text("""
                SELECT DISTINCT
                    s.stop_id,
                    s.name,
                    s.lat,
                    s.lon
                FROM stops s
                JOIN stop_times st ON s.stop_id = st.stop_id
                JOIN trips t ON st.trip_id = t.trip_id
                JOIN routes r ON t.route_id = r.route_id
                WHERE r.route_id = ANY(:route_ids) OR r.route_short_name = ANY(:route_ids)
                ORDER BY s.name
            """)
            result = conn.execute(query, {"route_ids": route_id_list})
        else:
            query = text("""
                SELECT 
                    stop_id,
                    name,
                    lat,
                    lon
                FROM stops
                ORDER BY name
            """)
            result = conn.execute(query)
        
        return [dict(row._mapping) for row in result]

@app.get("/stats")
async def get_system_stats():
    """Get overall system statistics"""
    with engine.connect() as conn:
        query = text("""
            SELECT 
                COUNT(DISTINCT vehicle_id) as total_vehicles,
                COUNT(DISTINCT route_id) as total_routes,
                AVG(speed_mph) as avg_speed,
                MAX(timestamp) as last_update
            FROM vehicles
            WHERE timestamp = (SELECT MAX(timestamp) FROM vehicles)
        """)
        result = conn.execute(query)
        row = result.fetchone()
        
        return dict(row._mapping)
