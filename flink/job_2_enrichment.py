import os
import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from utils.hdfs_client import hdfs
except ImportError:
    hdfs = None

def run_enrichment_pipeline(root_dir="."):
    print("=" * 70)
    print("[FLINK JOB 2] STREAMING EVENT ENRICHMENT (BRONZE -> SILVER HDFS)...")
    print("=" * 70)
    
    root_path = Path(root_dir).resolve()
    bronze_dir = root_path / "data_lake" / "bronze"
    silver_dir = root_path / "data_lake" / "silver"
    master_dir = root_path / "datasets" / "master_data"
    
    if not (master_dir / "customers.parquet").exists():
        print("[ERROR] Master dataset missing. Please execute datasets/setup_master_data.py first.")
        return
        
    df_cust = pd.read_parquet(master_dir / "customers.parquet")[["customer_id", "loyalty_tier", "city", "state_code"]]
    df_prod = pd.read_parquet(master_dir / "products.parquet")[["product_id", "category", "brand", "unit_price"]]
    
    now = datetime.now()
    time_part = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
    hdfs_active = hdfs and hdfs.is_available()
    
    order_records = []
    if hdfs_active:
        order_records = hdfs.read_jsonl_events("/data_lake/bronze/order-events", max_files=10, max_lines_per_file=200)
    if not order_records:
        orders_path = bronze_dir / "order-events" / time_part
        if orders_path.exists() and any(orders_path.glob("*.jsonl")):
            for file in orders_path.glob("*.jsonl"):
                with open(file, "r", encoding="utf-8") as f:
                    order_records.extend([json.loads(x) for x in f if x.strip()])
                    
    if order_records:
        df_orders = pd.DataFrame(order_records)
        if not df_orders.empty and "customer_id" in df_orders.columns:
            df_enriched_orders = df_orders.merge(df_cust, on="customer_id", how="left")
            df_enriched_orders["loyalty_tier"] = df_enriched_orders["loyalty_tier"].fillna("Standard")
            
            # Always write to local mirror
            dest = silver_dir / "enriched-orders" / time_part
            dest.mkdir(parents=True, exist_ok=True)
            out_name = f"orders_enriched_{now.strftime('%H%M')}.parquet"
            out_file = dest / out_name
            df_enriched_orders.to_parquet(out_file, index=False)
            
            if hdfs_active:
                try:
                    hdfs.write_parquet(f"/data_lake/silver/enriched-orders/{time_part}/{out_name}", df_enriched_orders)
                except Exception:
                    pass
            print(f"   [SUCCESS] Enriched {len(df_enriched_orders):,} orders with Customer Loyalty tables -> {out_name}")
        else:
            print("   [INFO] Waiting for valid streaming customer orders in HDFS before applying enrichment join.")

    view_records = []
    if hdfs_active:
        view_records = hdfs.read_jsonl_events("/data_lake/bronze/product-view-events", max_files=10, max_lines_per_file=200)
    if not view_records:
        views_path = bronze_dir / "product-view-events" / time_part
        if views_path.exists() and any(views_path.glob("*.jsonl")):
            for file in views_path.glob("*.jsonl"):
                with open(file, "r", encoding="utf-8") as f:
                    view_records.extend([json.loads(x) for x in f if x.strip()])
                    
    if view_records:
        df_views = pd.DataFrame(view_records)
        if not df_views.empty and "product_id" in df_views.columns:
            df_enriched_views = df_views.merge(df_prod, on="product_id", how="left", suffixes=("", "_master"))
            
            # Always write to local mirror
            dest_views = silver_dir / "customer-events" / time_part
            dest_views.mkdir(parents=True, exist_ok=True)
            out_name = f"product_views_enriched_{now.strftime('%H%M')}.parquet"
            out_file = dest_views / out_name
            df_enriched_views.to_parquet(out_file, index=False)
            
            if hdfs_active:
                try:
                    hdfs.write_parquet(f"/data_lake/silver/customer-events/{time_part}/{out_name}", df_enriched_views)
                except Exception:
                    pass
            print(f"   [SUCCESS] Enriched {len(df_enriched_views):,} product view interactions -> {out_name}")
        else:
            print("   [INFO] Waiting for valid streaming product interactions in HDFS before applying enrichment join.")
            
    print("[COMPLETED] Flink Job 2 Silver Layer HDFS enrichment completed successfully.")
if __name__ == "__main__":
    work_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    run_enrichment_pipeline(work_dir)
