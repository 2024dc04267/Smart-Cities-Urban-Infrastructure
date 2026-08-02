import json
import time
import logging
import threading
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(threadName)s] - %(message)s')
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'urbanpulse.traffic_signals'

def run_high_priority_consumer():
    """HIGH_PRIORITY Consumer: Real-time traffic automation (Zero Delay)"""
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'HIGH_PRIORITY',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    
    logging.info("[HIGH_PRIORITY] Started. Processing immediately...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logging.error(f"[HIGH_PRIORITY] Error: {msg.error()}")
                continue
            
            # Fast processing - Real-time Signal Controller Logic
            data = json.loads(msg.value().decode('utf-8'))
            # Near-zero processing latency
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def run_standard_priority_consumer(consumer_id):
    """STANDARD_PRIORITY Consumer: Analytics dashboard (Simulated Slowdown)"""
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'STANDARD_PRIORITY',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    
    thread_name = f"Standard-Consumer-{consumer_id}"
    logging.info(f"[{thread_name}] Started.")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logging.error(f"[{thread_name}] Error: {msg.error()}")
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            
            # Artificial processing delay to demonstrate lag accumulation
            time.sleep(1.5)
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    logging.info("Starting Traffic Signal Priority Consumer Demonstration...")
    
    # 1. Spin up High Priority Consumer Thread
    high_prio_thread = threading.Thread(
        target=run_high_priority_consumer, 
        name="HighPriorityThread", 
        daemon=True
    )
    high_prio_thread.start()
    
    # 2. Spin up 3 Standard Priority Consumer Threads
    std_threads = []
    for i in range(3):
        t = threading.Thread(
            target=run_standard_priority_consumer, 
            args=(i,), 
            name=f"StandardThread-{i}", 
            daemon=True
        )
        t.start()
        std_threads.append(t)
        
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping priority consumers...")