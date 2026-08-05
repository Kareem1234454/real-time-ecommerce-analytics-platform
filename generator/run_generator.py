import os
import sys
import time
import json
import yaml
import random
import pandas as pd
from pathlib import Path
from datetime import datetime
from event_builder import EventBuilder
from session_controller import SessionController
from scenario_engine import ScenarioEngine

try:
    from confluent_kafka import Producer
    HAS_CONFLUENT = True
except ImportError:
    HAS_CONFLUENT = False
    try:
        from kafka import KafkaProducer
        HAS_KAFKA_PY = True
    except ImportError:
        HAS_KAFKA_PY = False

def load_master_data(root_dir):
    master_path = Path(root_dir) / "datasets" / "master_data"
    cust_file = master_path / "customers.parquet"
    prod_file = master_path / "products.parquet"
    if not cust_file.exists() or not prod_file.exists():
        print(f"[ERROR] Master datasets not found in {master_path}. Run setup_master_data.py first.")
        sys.exit(1)
    df_cust = pd.read_parquet(cust_file)
    df_prod = pd.read_parquet(prod_file)
    print(f"[SUCCESS] Loaded {len(df_cust):,} Customers and {len(df_prod):,} Products into simulation cache.")
    return df_cust, df_prod

try:
    from utils.hdfs_client import hdfs
except ImportError:
    hdfs = None

_hdfs_last_check = 0
_hdfs_available_cached = False
def check_hdfs_cached():
    global _hdfs_last_check, _hdfs_available_cached
    import time
    now_ts = time.time()
    if now_ts - _hdfs_last_check > 10.0:
        _hdfs_last_check = now_ts
        _hdfs_available_cached = (hdfs is not None) and hdfs.is_available()
    return _hdfs_available_cached

def save_to_bronze_lake(root_dir, topic, payload):
    try:
        now = datetime.now()
        h_dir = f"/data_lake/bronze/{topic}/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
        h_file = f"{topic}_{now.strftime('%Y%m%d_%H')}.jsonl"
        
        # 1. Always write to local storage mirror to guarantee 100% data availability for Flink, Spark, and Streamlit UI
        folder = Path(root_dir) / "data_lake" / "bronze" / topic / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}" / f"hour={now.hour:02d}"
        folder.mkdir(parents=True, exist_ok=True)
        filename = folder / h_file
        with open(filename, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
            
        # 2. Synchronously mirror direct stream payload into HDFS storage blocks if cluster is online (using 10-sec cache)
        if check_hdfs_cached():
            try:
                hdfs.append_jsonl(h_dir, h_file, payload)
            except Exception:
                pass
    except Exception as e:
        pass

def run_simulation_loop():
    print("=" * 70)
    print("[START] STARTING REAL-TIME E-COMMERCE EVENT GENERATOR ENGINE...")
    print("=" * 70)
    
    work_dir = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config_path = work_dir / "generator" / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
        
    df_cust, df_prod = load_master_data(work_dir)
    eb = EventBuilder(source="web_sim_engine", event_version="1.0")
    engine = ScenarioEngine(config)
    session_ctrl = SessionController(df_cust, df_prod, eb, config.get("probabilities", {}))
    
    broker = config.get("kafka", {}).get("bootstrap_servers", "localhost:9092")
    producer = None
    
    if HAS_CONFLUENT:
        try:
            producer = Producer({'bootstrap.servers': broker})
            print(f"[SUCCESS] Connected to Kafka via confluent_kafka Producer at {broker}")
        except Exception:
            print("[WARNING] Could not connect via Confluent Kafka producer. Running with local Data Lake logging.")
    elif HAS_KAFKA_PY:
        try:
            producer = KafkaProducer(bootstrap_servers=broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            print(f"[SUCCESS] Connected to Kafka via KafkaProducer at {broker}")
        except Exception:
            print("[WARNING] Could not connect via kafka-python producer. Running with local Data Lake logging.")
            
    write_lake = config.get("kafka", {}).get("write_to_bronze_lake", True)
    stdout_log = config.get("kafka", {}).get("enable_stdout_log", True)
    
    event_count = 0
    start_time = time.time()
    
    print(f"\n[RUNNING] Simulation Active in mode: [{engine.current_mode.upper()}] | Target Rate: ~{engine.get_effective_rate()} evt/s")
    print("PRESS Ctrl+C TO STOP GENERATOR\n")
    
    try:
        while True:
            target_rate = engine.get_effective_rate()
            sleep_interval = 1.0 / max(target_rate, 1)
            
            is_fraud = engine.should_inject_fraud()
            topic, payload = session_ctrl.advance_session(inject_fraud=is_fraud)
            
            if producer and HAS_CONFLUENT:
                try:
                    producer.produce(topic, json.dumps(payload).encode('utf-8'))
                    producer.poll(0)
                except Exception:
                    pass
            elif producer and HAS_KAFKA_PY:
                try:
                    producer.send(topic, value=payload)
                except Exception:
                    pass
                    
            if write_lake:
                save_to_bronze_lake(work_dir, topic, payload)
                
            event_count += 1
            if stdout_log and (event_count % 5 == 0 or is_fraud):
                status_icon = "[FRAUD ALARM]" if is_fraud else "[EVENT PUBLISHED]"
                print(f"{status_icon:17} Topic: {topic:18} | Type: {payload.get('event_type'):16} | Customer: {payload.get('customer_id')}")
                
            time.sleep(sleep_interval)
            
    except KeyboardInterrupt:
        print("\n[PAUSED] Simulation paused by user.")
        if producer and HAS_CONFLUENT:
            producer.flush()
        elif producer and HAS_KAFKA_PY:
            producer.close()
        elapsed = round(time.time() - start_time, 2)
        print(f"[COMPLETED] Generated {event_count:,} total events in {elapsed}s (~{int(event_count/max(1, elapsed))} evt/s).")

if __name__ == "__main__":
    run_simulation_loop()
