from fastapi import FastAPI
import asyncpg
import os

# Load environment variables
DB_HOST = os.getenv("DB_HOST", "dpg-cv85t1t6l47c73f4toqg-a.oregon-postgres.render.com")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "spy_data")
DB_USER = os.getenv("DB_USER", "spy_data_user")
DB_PASS = os.getenv("DB_PASS", "UgqWwwN107nzxvryK43WZUS6t44hxFJF")

app = FastAPI()

# Database connection pool
async def connect_db():
    return await asyncpg.create_pool(
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        host=DB_HOST,
        port=DB_PORT,
        ssl="require"
    )

@app.get("/")
async def root():
    return {"message": "SPY Options & OHLC API is running!"}

# Fetch latest OHLC data
@app.get("/spy/ohlc/latest")
async def get_latest_ohlc():
    pool = await connect_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM spy_ohlc ORDER BY recorded_at DESC LIMIT 5;")
    return {"data": [dict(row) for row in rows]}

# Fetch top 3 call and top 3 put levels by volume
@app.get("/spy/options/top")
async def get_top_option_levels():
    pool = await connect_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            (SELECT * FROM spy_option_data ORDER BY call_volume DESC LIMIT 3)
            UNION
            (SELECT * FROM spy_option_data ORDER BY put_volume DESC LIMIT 3);
        """)
    return {"data": [dict(row) for row in rows]}

# Fetch most recent 5 option price levels
@app.get("/spy/options/recent")
async def get_recent_option_levels():
    pool = await connect_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM spy_option_data ORDER BY recorded_at DESC LIMIT 5;")
    return {"data": [dict(row) for row in rows]}
