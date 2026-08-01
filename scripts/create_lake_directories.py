import os
from pathlib import Path
from datetime import datetime

def create_medallion_architecture(root_dir="."):
    print("=" * 70)
    print("[START] INITIALIZING MEDALLION DATA LAKE FOLDER HIERARCHY (BRONZE -> SILVER -> GOLD)...")
    print("=" * 70)
    
    root_path = Path(root_dir).resolve()
    data_lake = root_path / "data_lake"
    
    now = datetime.now()
    year_str = str(now.year)
    month_str = f"{now.month:02d}"
    day_str = f"{now.day:02d}"
    hour_str = f"{now.hour:02d}"
    
    time_part = f"year={year_str}/month={month_str}/day={day_str}/hour={hour_str}"
    
    # 1. Bronze Layer (Raw JSON Event Streams)
    bronze_topics = [
        "search-events", "product-view-events", "cart-events", 
        "checkout-events", "payment-events", "order-events", 
        "review-events", "inventory-events", "dead-letter-events"
    ]
    for topic in bronze_topics:
        topic_path = data_lake / "bronze" / topic / time_part
        topic_path.mkdir(parents=True, exist_ok=True)
        (topic_path / ".keep").touch(exist_ok=True)
        
    print(f"[SUCCESS] Created 9 Bronze Layer (Raw Stream) partitioned directories under {data_lake / 'bronze'}")

    # 2. Silver Layer (Validated & Enriched Parquet Datasets)
    silver_domains = [
        "customer-events", "enriched-orders", "sessions", "inventory-updates"
    ]
    for domain in silver_domains:
        domain_path = data_lake / "silver" / domain / time_part
        domain_path.mkdir(parents=True, exist_ok=True)
        (domain_path / ".keep").touch(exist_ok=True)
        
    print(f"[SUCCESS] Created 4 Silver Layer (Enriched Parquet) partitioned directories under {data_lake / 'silver'}")

    # 3. Gold Layer (Aggregated KPIs & Business Intelligence Ready)
    gold_kpi_folders = [
        "revenue_per_hour", "top_selling_products", "customer_lifetime_value",
        "cart_abandonment", "executive_dashboard", "fraud_alerts_log"
    ]
    for kpi in gold_kpi_folders:
        kpi_path = data_lake / "gold" / kpi / f"year={year_str}"
        kpi_path.mkdir(parents=True, exist_ok=True)
        (kpi_path / ".keep").touch(exist_ok=True)
        
    print(f"[SUCCESS] Created 6 Gold Layer (KPI & Analytics) directories under {data_lake / 'gold'}")
    print("\n[COMPLETED] DATA LAKE INITIALIZATION SUCCESSFUL! Medallion structure prepared for streaming sinks.")

if __name__ == "__main__":
    work_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    create_medallion_architecture(work_dir)
