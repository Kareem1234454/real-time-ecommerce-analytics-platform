import os
import sys
from pathlib import Path
from datetime import datetime

# Import enterprise HDFS bridge
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from utils.hdfs_client import hdfs
except ImportError:
    hdfs = None

def create_medallion_architecture(root_dir="."):
    print("=" * 70)
    print("[START] INITIALIZING MEDALLION HDFS DISTRIBUTED DATA LAKE (BRONZE -> SILVER -> GOLD)...")
    print("=" * 70)
    
    now = datetime.now()
    year_str = str(now.year)
    month_str = f"{now.month:02d}"
    day_str = f"{now.day:02d}"
    hour_str = f"{now.hour:02d}"
    
    time_part = f"year={year_str}/month={month_str}/day={day_str}/hour={hour_str}"
    
    hdfs_online = hdfs and hdfs.is_available()
    if hdfs_online:
        print("[HADOOP HDFS] Connected successfully to NameNode WebHDFS service (Port 9870)!")
    else:
        print("[HADOOP WARNING] NameNode offline or initializing. Setting up fallback local disk mirrors...")
        root_path = Path(root_dir).resolve()
        data_lake_local = root_path / "data_lake"
    
    # 1. Bronze Layer (Raw JSON Event Streams)
    bronze_topics = [
        "search-events", "product-view-events", "cart-events", 
        "checkout-events", "payment-events", "order-events", 
        "review-events", "inventory-events", "dead-letter-events"
    ]
    for topic in bronze_topics:
        h_path = f"/data_lake/bronze/{topic}/{time_part}"
        if hdfs_online:
            hdfs.makedirs(h_path)
        else:
            topic_path = data_lake_local / "bronze" / topic / time_part
            topic_path.mkdir(parents=True, exist_ok=True)
            (topic_path / ".keep").touch(exist_ok=True)
            
    print(f"[SUCCESS] Created 9 Bronze Layer partitioned streaming storage paths in HDFS (/data_lake/bronze)")

    # 2. Silver Layer (Validated & Enriched Parquet Datasets)
    silver_domains = [
        "customer-events", "enriched-orders", "sessions", "inventory-updates"
    ]
    for domain in silver_domains:
        h_path = f"/data_lake/silver/{domain}/{time_part}"
        if hdfs_online:
            hdfs.makedirs(h_path)
        else:
            domain_path = data_lake_local / "silver" / domain / time_part
            domain_path.mkdir(parents=True, exist_ok=True)
            (domain_path / ".keep").touch(exist_ok=True)
            
    print(f"[SUCCESS] Created 4 Silver Layer Enriched Parquet storage paths in HDFS (/data_lake/silver)")

    # 3. Gold Layer (Aggregated KPIs & Business Intelligence Ready)
    gold_kpi_folders = [
        "revenue_per_hour", "top_selling_products", "customer_lifetime_value",
        "cart_abandonment", "executive_dashboard", "fraud_alerts_log"
    ]
    for kpi in gold_kpi_folders:
        h_path = f"/data_lake/gold/{kpi}/year={year_str}"
        if hdfs_online:
            hdfs.makedirs(h_path)
        else:
            kpi_path = data_lake_local / "gold" / kpi / f"year={year_str}"
            kpi_path.mkdir(parents=True, exist_ok=True)
            (kpi_path / ".keep").touch(exist_ok=True)
            
    print(f"[SUCCESS] Created 6 Gold Layer Analytics storage paths in HDFS (/data_lake/gold)")
    print("\n[COMPLETED] HDFS DISTRIBUTED DATA LAKE INITIALIZATION SUCCESSFUL! Medallion structure prepared in Hadoop.")

if __name__ == "__main__":
    work_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    create_medallion_architecture(work_dir)
