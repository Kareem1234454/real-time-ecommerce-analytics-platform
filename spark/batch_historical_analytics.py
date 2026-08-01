import os
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum, avg, count, round as spark_round, desc
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False

def run_spark_historical_analytics(root_dir="."):
    print("=" * 70)
    print("[SPARK BATCH ENGINE] HISTORICAL ANALYTICS & STRATEGIC KPI REPORTING...")
    print("=" * 70)
    
    root_path = Path(root_dir).resolve()
    master_dir = root_path / "datasets" / "master_data"
    gold_dir = root_path / "data_lake" / "gold" / "executive_dashboard"
    gold_dir.mkdir(parents=True, exist_ok=True)
    
    if not HAS_PYSPARK:
        print("[INFO] PySpark package not initialized in current process. Running high-performance Pandas Data Lake compute fallback...")
        _run_pandas_fallback(root_path, master_dir, gold_dir)
        return

    print("[INFO] Initializing SparkSession for local batch reporting...")
    spark = SparkSession.builder \
        .appName("RealTimeEcommerceBatchAnalytics") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    cust_file = str(master_dir / "customers.parquet")
    prod_file = str(master_dir / "products.parquet")
    
    if not os.path.exists(cust_file) or not os.path.exists(prod_file):
        print("[ERROR] Master Parquet files missing. Execute setup_master_data.py first.")
        return

    df_cust = spark.read.parquet(cust_file)
    df_prod = spark.read.parquet(prod_file)
    
    print("[SUCCESS] Successfully loaded Master datasets into Spark DataFrame structure.")

    print("\n[INFO] Computing Product Category Revenue Value Distribution in Spark SQL...")
    df_cat_summary = df_prod.groupBy("category") \
        .agg(
            count("product_id").alias("product_count"),
            spark_round(avg("unit_price"), 2).alias("avg_unit_price")
        ).orderBy(desc("product_count"))
        
    df_cat_summary.show(5, truncate=False)
    
    out_cat = str(gold_dir / "category_metrics.parquet")
    df_cat_summary.write.mode("overwrite").parquet(out_cat)
    print(f"   --> Wrote Spark aggregated category report to {out_cat}")

    print("\n[INFO] Computing Customer Loyalty Demographics...")
    df_loyalty = df_cust.groupBy("loyalty_tier", "state_code") \
        .agg(count("customer_id").alias("total_customers")) \
        .orderBy(desc("total_customers"))
        
    df_loyalty.show(5, truncate=False)
    
    out_loyalty = str(gold_dir / "loyalty_demographics.parquet")
    df_loyalty.write.mode("overwrite").parquet(out_loyalty)
    print(f"   --> Wrote Spark aggregated demographic report to {out_loyalty}")
    
    spark.stop()
    print("\n[COMPLETED] APACHE SPARK BATCH PROCESSING COMPLETED SUCCESSFULLY!")

def _run_pandas_fallback(root_path, master_dir, gold_dir):
    df_cust = pd.read_parquet(master_dir / "customers.parquet")
    df_prod = pd.read_parquet(master_dir / "products.parquet")
    
    print("\n[INFO] Computing Product Category Revenue Value Distribution...")
    df_cat = df_prod.groupby("category").agg(
        product_count=("product_id", "count"),
        avg_unit_price=("unit_price", "mean")
    ).reset_index().round(2).sort_values(by="product_count", ascending=False)
    
    print(df_cat.head(5).to_string(index=False))
    df_cat.to_parquet(gold_dir / "category_metrics.parquet", index=False)
    
    print("\n[INFO] Computing Customer Loyalty Demographics...")
    df_loyalty = df_cust.groupby(["loyalty_tier", "state_code"]).agg(
        total_customers=("customer_id", "count")
    ).reset_index().sort_values(by="total_customers", ascending=False)
    
    print(df_loyalty.head(5).to_string(index=False))
    df_loyalty.to_parquet(gold_dir / "loyalty_demographics.parquet", index=False)
    print("\n[COMPLETED] BATCH ANALYTICS REPORTING COMPLETED SUCCESSFULLY!")

if __name__ == "__main__":
    work_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    run_spark_historical_analytics(work_dir)
