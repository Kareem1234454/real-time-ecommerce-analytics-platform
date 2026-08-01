import os
import sys
import yaml
from pathlib import Path
import time

try:
    from confluent_kafka.admin import AdminClient, NewTopic
    HAS_CONFLUENT = True
except ImportError:
    HAS_CONFLUENT = False
    try:
        from kafka.admin import KafkaAdminClient, NewTopic as KafkaNewTopic
        HAS_KAFKA_PY = True
    except ImportError:
        HAS_KAFKA_PY = False

def create_kafka_topics(config_path="topics.yaml", bootstrap_servers="localhost:9092"):
    print("=" * 70)
    print("[START] PROVISIONING APACHE KAFKA STREAMING TOPICS...")
    print("=" * 70)
    
    yaml_path = Path(__file__).parent / "topics.yaml"
    if not yaml_path.exists():
        print(f"[ERROR] Configuration file not found: {yaml_path}")
        return False
        
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    topic_defs = config.get("topics", [])
    
    if not HAS_CONFLUENT and not HAS_KAFKA_PY:
        print("[WARNING] Neither 'confluent-kafka' nor 'kafka-python' packages found.")
        print("[HINT] Please install via 'pip install -r requirements.txt'")
        return False

    print(f"[INFO] Connecting to Kafka broker at {bootstrap_servers}...")

    if HAS_CONFLUENT:
        try:
            admin = AdminClient({'bootstrap.servers': bootstrap_servers})
            existing = admin.list_topics(timeout=5).topics
            new_topics = []
            for t in topic_defs:
                t_name = t["name"]
                if t_name not in existing:
                    new_topics.append(NewTopic(t_name, num_partitions=t["partitions"], replication_factor=t["replication_factor"]))
                else:
                    print(f"   [INFO] Topic '{t_name}' already exists.")
            if new_topics:
                fs = admin.create_topics(new_topics)
                for topic, f in fs.items():
                    try:
                        f.result()
                        print(f"   [SUCCESS] Successfully created topic: {topic}")
                    except Exception as e:
                        print(f"   [ERROR] Failed to create topic {topic}: {e}")
            else:
                print("[COMPLETED] All Kafka topics are already operational!")
            return True
        except Exception as e_con:
            print(f"[WARNING] Could not connect to Kafka via confluent-kafka: {e_con}")
            print("[HINT] Ensure Docker service 'kafka' is operational via 'docker-compose up -d'.")
            return False
    elif HAS_KAFKA_PY:
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="topic_creator", request_timeout_ms=5000)
            existing = admin.list_topics()
            new_topics = []
            for t in topic_defs:
                t_name = t["name"]
                if t_name not in existing:
                    new_topics.append(KafkaNewTopic(name=t_name, num_partitions=t["partitions"], replication_factor=t["replication_factor"]))
                else:
                    print(f"   [INFO] Topic '{t_name}' already exists.")
            if new_topics:
                admin.create_topics(new_topics)
                for nt in new_topics:
                    print(f"   [SUCCESS] Successfully created topic: {nt.name}")
            else:
                print("[COMPLETED] All Kafka topics are already operational!")
            admin.close()
            return True
        except Exception as e_kpy:
            print(f"[WARNING] Could not connect to Kafka via kafka-python: {e_kpy}")
            print("[HINT] Ensure Docker service 'kafka' is operational via 'docker-compose up -d'.")
            return False

if __name__ == "__main__":
    broker_url = os.getenv("KAFKA_BROKER", "localhost:9092")
    create_kafka_topics(bootstrap_servers=broker_url)
