from fastapi import FastAPI, HTTPException  # Add HTTPException import
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager  # Add this import
import json
import os
from pathlib import Path
from databases import Database
from dotenv import load_dotenv

load_dotenv()
HOT_MUNI_DATA = os.getenv("HOT_MUNI_DATA")
psql_db_name = os.environ.get("TRANSIT_DB_NAME")
psql_username = os.environ.get("PSQL_USERNAME")
static_output_dir = os.environ.get("STATIC_MUNI_DATA")
data_dir = Path(static_output_dir) / "data"

class RouteGeometry(BaseModel):
    route_id: str
    geometry: list[dict]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection manager
@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host="172.19.0.1",  # Your Docker bridge IP
            dbname=psql_db_name,
            user=psql_username,
            port=5432,
            cursor_factory=RealDictCursor
        )
        yield conn
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if conn:
            conn.close()

@app.get("/")
async def root():
    return {"message": "MUNI API is running", "hot_data_path": HOT_MUNI_DATA}

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "hot_data_path": HOT_MUNI_DATA,
        "file_exists": os.path.exists(HOT_MUNI_DATA) if HOT_MUNI_DATA else False
    }

@app.get("/debug")
async def debug():
    return {
        "HOT_MUNI_DATA": HOT_MUNI_DATA,
        "file_exists": os.path.exists(HOT_MUNI_DATA) if HOT_MUNI_DATA else False,
        "cwd": os.getcwd(),
        "env_vars": dict(os.environ)
    }

@app.get("/hot-data")
async def get_hot_data():
    if not os.path.exists(HOT_MUNI_DATA):
        return JSONResponse(content={"error": "No data yet"}, status_code=404)
    try:
        with open(HOT_MUNI_DATA, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

# Fixed endpoint
@app.get('/routes/{route_id}', response_model=RouteGeometry)  # @app not @api
async def get_route_stats(route_id):  # async not asynch
    # Regular string, not triple quotes
    query = """
        SELECT 
            rs.route_id,
            s.shape_id,
            ST_AsGeoJSON(s.route_line) as geometry
        FROM route_shapes rs
        JOIN shapes s ON rs.shape_id = s.shape_id
        WHERE rs.route_id = %s
    """
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (route_id,))
            results = cursor.fetchall()
        
        if not results:
            raise HTTPException(status_code=404, detail="Route not found")
        
        return RouteGeometry(
        route_id=route_id,
        geometry=[
            {
                "shape_id": row['shape_id'], 
                "geometry": json.loads(row['geometry'])
            }
            for row in results
        ]
)