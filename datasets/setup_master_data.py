import os
import sys
import pandas as pd
import numpy as np
import json
from pathlib import Path
from faker import Faker
import random

def create_master_datasets(root_dir="."):
    print("=" * 70)
    print("[START] INITIALIZING OLIST MASTER DATASET PROCESSING & ENRICHMENT STAGING...")
    print("=" * 70)
    
    root_path = Path(root_dir).resolve()
    kaggle_dir = root_path / "kaggle_data"
    output_dir = root_path / "datasets" / "master_data"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    required_files = [
        "olist_customers_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "product_category_name_translation.csv",
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv"
    ]
    
    for filename in required_files:
        if not (kaggle_dir / filename).exists():
            raise FileNotFoundError(f"[ERROR] Missing required Kaggle dataset file: {kaggle_dir / filename}")
            
    print(f"[SUCCESS] Verified all required CSV datasets in: {kaggle_dir}")
    
    # 1. Process Customers Master Data
    print("\n[INFO] Processing Customers Master Data...")
    df_customers_raw = pd.read_csv(kaggle_dir / "olist_customers_dataset.csv")
    df_customers = df_customers_raw[[
        "customer_id", "customer_unique_id", "customer_city", "customer_state"
    ]].copy()
    df_customers.rename(columns={
        "customer_city": "city",
        "customer_state": "state_code"
    }, inplace=True)
    
    np.random.seed(42)
    loyalty_tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    df_customers["loyalty_tier"] = np.random.choice(loyalty_tiers, size=len(df_customers), p=[0.5, 0.3, 0.15, 0.05])
    df_customers["country"] = "Brazil"
    df_customers["signup_year"] = np.random.choice([2022, 2023, 2024, 2025], size=len(df_customers))
    
    cust_parquet = output_dir / "customers.parquet"
    cust_json = output_dir / "customers.json"
    df_customers.to_parquet(cust_parquet, index=False)
    df_customers.head(5000).to_json(cust_json, orient="records", lines=True)
    print(f"   --> Saved {len(df_customers):,} Customers to {cust_parquet} and {cust_json}")

    # 2. Process Products & Categories Master Data
    print("\n[INFO] Processing Products & Category Translation Data...")
    df_products_raw = pd.read_csv(kaggle_dir / "olist_products_dataset.csv")
    df_trans = pd.read_csv(kaggle_dir / "product_category_name_translation.csv")
    
    df_products = df_products_raw.merge(
        df_trans, on="product_category_name", how="left"
    )
    df_products["category"] = df_products["product_category_name_english"].fillna("General_Merchandise")
    
    df_items = pd.read_csv(kaggle_dir / "olist_order_items_dataset.csv")
    df_prices = df_items.groupby("product_id")["price"].mean().reset_index()
    df_prices.rename(columns={"price": "unit_price"}, inplace=True)
    
    df_products = df_products.merge(df_prices, on="product_id", how="left")
    df_products["unit_price"] = df_products["unit_price"].fillna(49.99).round(2)
    df_products["brand"] = "Olist_Certified_" + df_products["category"].str[:4].str.upper()
    df_products["currency"] = "BRL"
    
    df_products_clean = df_products[[
        "product_id", "category", "brand", "unit_price", "currency", 
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
    ]].copy()
    
    prod_parquet = output_dir / "products.parquet"
    prod_json = output_dir / "products.json"
    df_products_clean.to_parquet(prod_parquet, index=False)
    df_products_clean.head(5000).to_json(prod_json, orient="records", lines=True)
    print(f"   --> Saved {len(df_products_clean):,} Products to {prod_parquet} and {prod_json}")

    # 3. Process Sellers Master Data
    print("\n[INFO] Processing Sellers Master Data...")
    df_sellers_raw = pd.read_csv(kaggle_dir / "olist_sellers_dataset.csv")
    df_sellers = df_sellers_raw[["seller_id", "seller_city", "seller_state"]].copy()
    df_sellers.rename(columns={"seller_city": "city", "seller_state": "state"}, inplace=True)
    df_sellers["seller_name"] = "Partner_Merchant_" + df_sellers["seller_id"].str[:6]
    df_sellers["country"] = "Brazil"
    df_sellers["rating"] = np.random.uniform(3.5, 5.0, size=len(df_sellers)).round(1)
    
    seller_parquet = output_dir / "sellers.parquet"
    seller_json = output_dir / "sellers.json"
    df_sellers.to_parquet(seller_parquet, index=False)
    df_sellers.to_json(seller_json, orient="records", lines=True)
    print(f"   --> Saved {len(df_sellers):,} Sellers to {seller_parquet}")

    # 4. Generate Warehouses & Inventory
    print("\n[INFO] Generating Synthetic Warehouses & Inventory Records...")
    fake = Faker('en_US')
    Faker.seed(42)
    
    warehouses = [
        {"warehouse_id": 1, "warehouse_name": "Sao Paulo Hub Central", "city": "sao paulo", "region": "Southeast", "capacity_units": 500000},
        {"warehouse_id": 2, "warehouse_name": "Rio de Janeiro Distribution Center", "city": "rio de janeiro", "region": "Southeast", "capacity_units": 350000},
        {"warehouse_id": 3, "warehouse_name": "Curitiba Southern Depot", "city": "curitiba", "region": "South", "capacity_units": 200000},
        {"warehouse_id": 4, "warehouse_name": "Salvador Northeastern Hub", "city": "salvador", "region": "Northeast", "capacity_units": 250000},
        {"warehouse_id": 5, "warehouse_name": "Brasilia Federal Terminal", "city": "brasilia", "region": "Central-West", "capacity_units": 300000}
    ]
    df_warehouses = pd.DataFrame(warehouses)
    wh_parquet = output_dir / "warehouses.parquet"
    wh_json = output_dir / "warehouses.json"
    df_warehouses.to_parquet(wh_parquet, index=False)
    df_warehouses.to_json(wh_json, orient="records", lines=True)
    
    inventory_records = []
    sample_prods = df_products_clean.head(5000)["product_id"].tolist()
    
    for i, pid in enumerate(sample_prods):
        for wh in warehouses:
            inventory_records.append({
                "inventory_id": f"INV-{i}-{wh['warehouse_id']}",
                "product_id": pid,
                "warehouse_id": wh["warehouse_id"],
                "quantity": int(np.random.randint(10, 500)),
                "reserved_quantity": int(np.random.randint(0, 5)),
                "last_updated": "2026-07-30T22:00:00Z"
            })
            
    df_inv = pd.DataFrame(inventory_records)
    inv_parquet = output_dir / "inventory.parquet"
    inv_json = output_dir / "inventory.json"
    df_inv.to_parquet(inv_parquet, index=False)
    df_inv.to_json(inv_json, orient="records", lines=True)
    print(f"   --> Generated {len(df_warehouses)} Warehouses and {len(df_inv):,} Inventory Stock lines.")
    
    print("\n[COMPLETED] MASTER DATASET PREPARATION COMPLETE! Ready for Flink/Postgres Enrichment.")

if __name__ == "__main__":
    work_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    create_master_datasets(work_dir)
