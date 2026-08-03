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
        
        # Self-healing table creation logic to ensure operational catalog tables exist
        print("[INFO] Verifying and generating PostgreSQL operational table schemas...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id VARCHAR(50) PRIMARY KEY,
                customer_unique_id VARCHAR(50),
                city VARCHAR(100),
                state_code VARCHAR(10),
                loyalty_tier VARCHAR(30),
                country VARCHAR(50),
                signup_year INTEGER
            );
            CREATE TABLE IF NOT EXISTS products (
                product_id VARCHAR(50) PRIMARY KEY,
                category VARCHAR(100),
                brand VARCHAR(100),
                unit_price NUMERIC(10, 2),
                currency VARCHAR(10),
                product_weight_g NUMERIC(10, 2),
                product_length_cm NUMERIC(10, 2),
                product_height_cm NUMERIC(10, 2),
                product_width_cm NUMERIC(10, 2)
            );
            CREATE TABLE IF NOT EXISTS sellers (
                seller_id VARCHAR(50) PRIMARY KEY,
                city VARCHAR(100),
                state VARCHAR(10),
                seller_name VARCHAR(100),
                country VARCHAR(50),
                rating NUMERIC(3, 1)
            );
            CREATE TABLE IF NOT EXISTS warehouses (
                warehouse_id INTEGER PRIMARY KEY,
                warehouse_name VARCHAR(100),
                city VARCHAR(100),
                region VARCHAR(50),
                capacity_units INTEGER
            );
            CREATE TABLE IF NOT EXISTS inventory (
                inventory_id VARCHAR(100) PRIMARY KEY,
                product_id VARCHAR(50),
                warehouse_id INTEGER,
                quantity INTEGER,
                reserved_quantity INTEGER,
                last_updated TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS fraud_alarms (
                alert_id VARCHAR(100) PRIMARY KEY,
                event_timestamp VARCHAR(50),
                customer_id VARCHAR(50),
                order_id VARCHAR(50),
                risk_score NUMERIC(5, 2),
                rule_violated VARCHAR(200),
                details TEXT
            );
        """)
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
