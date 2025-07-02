"""Send CSV rows to Kafka in micro‑batches of 100 lines every 10 seconds."""
import time, os, csv
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "student_loans_raw")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "10"))
DATA_DIR = os.getenv("DATA_DIR", "/data/raw/batch")

producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER],
                         value_serializer=lambda v: v.encode("utf-8"))

def stream_files():
    for file_name in sorted(os.listdir(DATA_DIR)):
        if not file_name.endswith(".csv"):
            continue
        with open(os.path.join(DATA_DIR, file_name), newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip header
            batch = []
            for row in reader:
                batch.append(",".join(row))
                if len(batch) == BATCH_SIZE:
                    for record in batch:
                        producer.send(TOPIC, record)
                    producer.flush()
                    batch.clear()
                    time.sleep(SLEEP_SECONDS)
            # send remaining
            for record in batch:
                producer.send(TOPIC, record)
            producer.flush()

if __name__ == "__main__":
    print("Starting producer…")
    stream_files()
    print("Done.")
