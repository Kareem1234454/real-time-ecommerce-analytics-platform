import os
import sys
import time

from job_1_validation_dlq import validate_and_route_events
from job_2_enrichment import run_enrichment_pipeline
from job_3_kpi_aggregations import compute_streaming_kpi_windows
from job_4_fraud_detection import detect_fraudulent_transactions

def run_continuous_stream_processing():
    print("=" * 70)
    print("   🚀 APACHE FLINK LIVE STREAMING & CEP WORKER CLUSTER ACTIVE")
    print("=======================================================================")
    print(" [STATUS] Continuous polling loop enabled over Medallion Data Lake.")
    print(" [STATUS] Evaluating Kafka streaming channels & detecting fraud attacks.")
    print("=======================================================================\n")
    
    work_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_conn = os.getenv("DATABASE_URL", "postgresql://admin:admin123@localhost:5432/ecommerce_meta")
    
    checkpoint_num = 1
    while True:
        try:
            print(f"\n--- [CHECKPOINT #{checkpoint_num}] Processing Streaming Windows ---")
            validate_and_route_events(work_dir)
            run_enrichment_pipeline(work_dir)
            compute_streaming_kpi_windows(work_dir)
            detect_fraudulent_transactions(db_conn, work_dir)
            checkpoint_num += 1
        except Exception as e:
            print(f" [WARNING] Streaming Checkpoint non-fatal exception: {e}")
            
        time.sleep(5)  # 5-second continuous streaming checkpoint interval

if __name__ == "__main__":
    run_continuous_stream_processing()
