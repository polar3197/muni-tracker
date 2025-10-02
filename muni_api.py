from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import os

POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')

# Database connection
DATABASE_URL = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "MUNI API is running"}

@app.get("/health")
async def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.get("/vehicles/current")
async def get_current_vehicles():
    """Get the latest position for each vehicle"""
    with engine.connect() as conn:
        query = text("""
            SELECT *
            FROM vehicles v
            INNER JOIN (
                SELECT vehicle_id, MAX(timestamp) as max_timestamp
                FROM vehicles
                GROUP BY vehicle_id
            ) latest ON v.vehicle_id = latest.vehicle_id 
                   AND v.timestamp = latest.max_timestamp
        """)
        result = conn.execute(query)
        return [dict(row._mapping) for row in result]

# Endpoint for getting stops along route
# @app.get('/routes/{route_id}', response_model=RouteGeometry)
# async def get_route_stats(route_id):
#     query = """
#         SELECT 
#             rs.route_id,
#             s.shape_id,
#             ST_AsGeoJSON(s.route_line) as geometry
#         FROM route_shapes rs
#         JOIN shapes s ON rs.shape_id = s.shape_id
#         WHERE rs.route_id = %s
#     """
    
#     with get_db_connection() as conn:
#         with conn.cursor() as cursor:
#             cursor.execute(query, (route_id,))
#             results = cursor.fetchall()
        
#         if not results:
#             raise HTTPException(status_code=404, detail="Route not found")
        
#         return RouteGeometry(
#         route_id=route_id,
#         geometry=[
#             {
#                 "shape_id": row['shape_id'], 
#                 "geometry": json.loads(row['geometry'])
#             }
#             for row in results
#         ]
# )