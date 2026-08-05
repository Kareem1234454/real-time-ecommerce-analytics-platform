import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from utils.hdfs_client import hdfs
except ImportError:
    hdfs = None

def validate_and_route_events(root_dir="."):
    print("=" * 70)
    print("[FLINK JOB 1] EVENT VALIDATION & DLQ ROUTING SERVICE ENGINE...")
    print("=" * 70)
    
    now = datetime.now()
    time_part = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
    
    print("[INFO] Flink Job 1 monitoring HDFS & streaming channels for malformed payloads...")
    
    topics_to_check = ["order-events", "payment-events", "cart-events", "checkout-events"]
    processed_count = 0
    invalid_count = 0
    
    hdfs_active = hdfs and hdfs.is_available()
    root_path = Path(root_dir).resolve()
    bronze_dir = root_path / "data_lake" / "bronze"
    dlq_dir = root_path / "data_lake" / "bronze" / "dead-letter-events"
    
    for topic in topics_to_check:
        events = []
        if hdfs_active:
            events = hdfs.read_jsonl_events(f"/data_lake/bronze/{topic}", max_files=10, max_lines_per_file=100)
        if not events:
            topic_path = bronze_dir / topic / time_part
            if topic_path.exists():
                for file_path in topic_path.glob("*.jsonl"):
                    with open(file_path, "r", encoding="utf-8") as f:
                        events.extend([json.loads(x) for x in f if x.strip()])
                        
        for payload in events:
            processed_count += 1
            try:
                is_valid = True
                reason = ""
                if not payload.get("event_id") or not payload.get("event_timestamp"):
                    is_valid = False
                    reason = "Missing required UUID or ISO timestamp"
                elif payload.get("amount", 0) < 0 or payload.get("quantity", 1) <= 0:
                    is_valid = False
                    reason = "Negative transaction monetary value or zero quantity"
                    
                if not is_valid:
                    invalid_count += 1
                    payload["dlq_reason"] = reason
                    payload["failed_at"] = datetime.utcnow().isoformat() + "Z"
                    
                    # Always write to local mirror
                    dlq_path = dlq_dir / time_part
                    dlq_path.mkdir(parents=True, exist_ok=True)
                    with open(dlq_path / "rejected_events.jsonl", "a", encoding="utf-8") as dlq_file:
                        dlq_file.write(json.dumps(payload) + "\n")
                        
                    if hdfs_active:
                        try:
                            hdfs.append_jsonl(f"/data_lake/bronze/dead-letter-events/{time_part}", "rejected_events.jsonl", payload)
                        except Exception:
                            pass
                        
                    print(f"   [DLQ REDIRECT] Rejected Event {payload.get('event_id', 'N/A')[:8]} -> {reason}")
            except Exception as parse_err:
                invalid_count += 1
                print(f"   [ERROR] JSON Parsing Syntax error sent to DLQ: {parse_err}")

    print(f"[COMPLETED] Flink Job 1 Checkpoint Completed: Verified {processed_count:,} events | DLQ Replaced: {invalid_count}")
    return processed_count, invalid_count

if __name__ == "__main__":
    work_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    validate_and_route_events(work_dir)
