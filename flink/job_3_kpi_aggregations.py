import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

def compute_streaming_kpi_windows(root_dir="."):
    print("=" * 70)
    print("[FLINK JOB 3] STATEFUL TUMBLING WINDOW KPI AGGREGATOR (SILVER -> GOLD)...")
    print("=" * 70)
    
    root_path = Path(root_dir).resolve()
    silver_dir = root_path / "data_lake" / "silver"
    bronze_dir = root_path / "data_lake" / "bronze"
    gold_dir = root_path / "data_lake" / "gold"
    
    now = datetime.now()
    year_part = f"year={now.year}"
    time_part = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
    
    silver_orders = list((silver_dir / "enriched-orders").rglob("*.parquet"))
    if not silver_orders:
        bronze_ord = list((bronze_dir / "order-events").rglob("*.jsonl"))
        if bronze_ord:
            records = []
            for f in bronze_ord:
                with open(f, "r", encoding="utf-8") as json_f:
                    records.extend([json.loads(x) for x in json_f if x.strip()])
            df = pd.DataFrame(records)
        else:
            df = pd.DataFrame()
    else:
        df = pd.concat([pd.read_parquet(p) for p in silver_orders])
        
    if not df.empty and "total_amount" in df.columns:
        total_rev = round(df["total_amount"].astype(float).sum(), 2)
        total_orders = len(df)
        aov = round(total_rev / max(total_orders, 1), 2)
        
        kpi_row = pd.DataFrame([{
            "window_end": now.isoformat(),
            "metric_type": "revenue_window_1h",
            "total_revenue_brl": total_rev,
            "total_orders": total_orders,
            "average_order_value": aov
        }])
        
        target_kpi = gold_dir / "revenue_per_hour" / year_part
        target_kpi.mkdir(parents=True, exist_ok=True)
        kpi_row.to_parquet(target_kpi / f"rev_kpi_{now.strftime('%Y%m%d_%H%M')}.parquet", index=False)
        print(f"   [KPI REVENUE] Computed Revenue KPI: BRL {total_rev:,} across {total_orders} orders (AOV: BRL {aov})")

    carts_path = bronze_dir / "cart-events" / time_part
    checkouts_path = bronze_dir / "checkout-events" / time_part
    
    total_carts = 0
    total_checkouts = 0
    
    if carts_path.exists():
        for file in carts_path.glob("*.jsonl"):
            with open(file, "r", encoding="utf-8") as f:
                total_carts += sum(1 for line in f if line.strip())
                
    if checkouts_path.exists():
        for file in checkouts_path.glob("*.jsonl"):
            with open(file, "r", encoding="utf-8") as f:
                total_checkouts += sum(1 for line in f if line.strip())
                
    abandonment_rate = 0.0
    if total_carts > 0:
        abandonment_rate = round(max(0, (total_carts - total_checkouts)) / total_carts * 100, 1)
        
    kpi_abn = pd.DataFrame([{
        "window_timestamp": now.isoformat(),
        "total_carts_initiated": total_carts,
        "total_checkouts_completed": total_checkouts,
        "cart_abandonment_percentage": abandonment_rate
    }])
    
    target_abn = gold_dir / "cart_abandonment" / year_part
    target_abn.mkdir(parents=True, exist_ok=True)
    kpi_abn.to_parquet(target_abn / f"abandonment_{now.strftime('%Y%m%d_%H%M')}.parquet", index=False)
    print(f"   [KPI ABANDONMENT] Computed Cart Abandonment Rate: {abandonment_rate}% ({total_carts} carts -> {total_checkouts} checkouts)")
    print("[COMPLETED] Flink Job 3 Gold Layer KPI aggregation completed successfully.")

if __name__ == "__main__":
    work_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    compute_streaming_kpi_windows(work_dir)
