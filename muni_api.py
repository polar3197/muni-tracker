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
from db_manager import DatabaseManager

POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
S3_BUCKET = os.getenv("S3_BUCKET")

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
            host="172.19.0.1",  # Docker bridge IP
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
        "file_exists": os.path.exists(HOT_MUNI_DATA) if HOT_MUNI_DATA else False
    }

@app.get("/hot-data")
async def get_hot_data():
    
    db_url = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"
    dm = DatabaseManager(
        db_url=db_url,
        s3_bucket=S3_BUCKET
    )
    
    if not os.path.exists(HOT_MUNI_DATA):
        return JSONResponse(content={"error": "No data yet"}, status_code=404)
    try:
        with open(HOT_MUNI_DATA, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

# Endpoint for getting stops along route
@app.get('/routes/{route_id}', response_model=RouteGeometry)
async def get_route_stats(route_id):
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