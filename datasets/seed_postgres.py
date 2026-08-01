import os
import sys
import pandas as pd
import psycopg2
from psycopg2 import pool, extras
from pathlib import Path

def seed_database(db_url="postgresql://admin:admin123@localhost:5432/ecommerce_meta", root_dir="."):
    print("=" * 70)
    print("[START] SEEDING POSTGRESQL OPERATIONAL METADATA TABLES FROM PARQUET...")
    print("=" * 70)
    
    root_path = Path(root_dir).resolve()
    master_dir = root_path / "datasets" / "master_data"
    
    if not master_dir.exists():
        print(f"[ERROR] Master data directory not found at {master_dir}. Please run setup_master_data.py first.")
        sys.exit(1)
        
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cursor = conn.cursor()
        print("[SUCCESS] Successfully connected to PostgreSQL instance at:", db_url.split("@")[-1])
    except Exception as e:
        print(f"[WARNING] Could not connect to PostgreSQL ({e}).")
        print("[HINT] Ensure Docker container 'postgres-meta' is running via 'docker-compose up -d'.")
        return False
        
    tables = [
        ("customers", "customers.parquet"),
        ("products", "products.parquet"),
        ("sellers", "sellers.parquet"),
        ("warehouses", "warehouses.parquet"),
        ("inventory", "inventory.parquet")
    ]
    
    for table_name, parquet_file in tables:
        file_path = master_dir / parquet_file
        if not file_path.exists():
            print(f"[WARNING] Skipping {table_name}: file {file_path} does not exist.")
            continue
            
        print(f"[INFO] Inserting data into PostgreSQL table: {table_name}...")
        df = pd.read_parquet(file_path)
        
        if table_name in ["customers", "inventory"] and len(df) > 10000:
            df = df.head(10000)
            
        columns = list(df.columns)
        values = [tuple(x) for x in df.to_numpy()]
        
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES %s
            ON CONFLICT DO NOTHING;
        """
        try:
            extras.execute_values(cursor, insert_query, values, page_size=1000)
            print(f"   --> Successfully loaded {len(values):,} rows into '{table_name}'.")
        except Exception as insert_err:
            print(f"   [ERROR] Failed to load '{table_name}': {insert_err}")
            
    cursor.close()
    conn.close()
    print("[COMPLETED] PostgreSQL Operational Database seeding completed successfully!")
    return True

if __name__ == "__main__":
    db_conn_str = os.getenv("DATABASE_URL", "postgresql://admin:admin123@localhost:5432/ecommerce_meta")
    work_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    seed_database(db_conn_str, work_dir)
