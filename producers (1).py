import time
import json
import random
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Delivery failed for record {msg.key()}: {err}")

# Task B Mandate: At-Least-Once + Idempotent Ingestion Configuration
producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'enable.idempotence': True,
    'acks': 'all',
    'retries': 5,
    'max.in.flight.requests.per.connection': 1
}
producer = Producer(producer_config)

def get_partition_key(topic, payload):
    """
    Task B Constraint Mapping:
    - bus_gps MUST use route_id to guarantee partition ordering per route.
    - air_quality uses sensor_id.
    - smart_meters uses ward_id.
    - traffic_signals uses junction_id.
    """
    if topic == 'urbanpulse.bus_gps':
        return str(payload.get("route_id", "UNKNOWN"))
    elif topic == 'urbanpulse.air_quality':
        return str(payload.get("sensor_id", "UNKNOWN"))
    elif topic == 'urbanpulse.smart_meters':
        return str(payload.get("ward_id", "UNKNOWN"))
    elif topic == 'urbanpulse.traffic_signals':
        return str(payload.get("junction_id", "UNKNOWN"))
    return "UNKNOWN"

def validate_and_route(topic, payload):
    """
    Validates messages against city rules. 
    Routes to original topic if valid, or urbanpulse.dlq if faulty.
    """
    errors = []
    
    # 1. Validation Rules (Task B DLQ Pattern)
    if topic == 'urbanpulse.air_quality':
        aqi = payload.get("aqi")
        if aqi is None:
            errors.append("MISSING_AQI_VALUE")
        elif aqi < 0 or aqi > 500:
            errors.append("AQI_OUT_OF_BOUNDS")
            
    elif topic == 'urbanpulse.bus_gps':
        lat = payload.get("lat")
        lon = payload.get("lon")
        if lat is None or lon is None or not (12.0 <= lat <= 14.0) or not (76.0 <= lon <= 78.0):
            errors.append("MALFORMED_GEOSPATIAL_COORDINATES")

    key_str = get_partition_key(topic, payload)

    # 2. Routing Logic
    if errors:
        # Route to Dead Letter Queue (DLQ)
        dlq_payload = {
            "original_topic": topic,
            "payload": payload,
            "error_reasons": errors,
            "failed_at": datetime.now(timezone.utc).isoformat()
        }
        producer.produce(
            topic='urbanpulse.dlq',
            key=key_str.encode('utf-8'),
            value=json.dumps(dlq_payload).encode('utf-8'),
            callback=delivery_report
        )
        return False
    else:
        # Route to original destination topic with correct key ordering
        producer.produce(
            topic=topic,
            key=key_str.encode('utf-8'),
            value=json.dumps(payload).encode('utf-8'),
            callback=delivery_report
        )
        return True

def simulate_urban_pulse():
    routes = ['R-101', 'R-202', 'R-303']
    sensors = ['AQI-S01', 'AQI-S02', 'AQI-S03']
    wards = ['Ward-Alpha', 'Ward-Beta', 'Ward-Gamma']
    junctions = ['JNC-01', 'JNC-02', 'JNC-03']
    
    logging.info("Starting validated UrbanPulse multi-source pipeline...")
    
    try:
        while True:
            timestamp = datetime.now(timezone.utc).isoformat()
            
            # --- Stream 1: Bus GPS ---
            route_id = random.choice(routes)
            bus_payload = {
                "bus_id": f"BUS-{random.randint(1000, 9999)}", 
                "route_id": route_id,
                "lat": round(random.uniform(12.90, 13.10), 4), 
                "lon": round(random.uniform(77.50, 77.70), 4),
                "speed_kmh": random.randint(0, 65), 
                "occupancy_pct": random.randint(10, 95), 
                "timestamp": timestamp
            }
            validate_and_route('urbanpulse.bus_gps', bus_payload)

            # --- Stream 2: Air Quality ---
            sensor_id = random.choice(sensors)
            aqi_val = None if random.random() < 0.05 else random.randint(45, 360) 
            aqi_payload = {
                "sensor_id": sensor_id, 
                "zone": f"Zone-{sensor_id.split('-')[-1]}",
                "pm25": random.randint(10, 120), 
                "pm10": random.randint(20, 200),
                "no2": random.randint(5, 50), 
                "aqi": aqi_val, 
                "timestamp": timestamp
            }
            if not validate_and_route('urbanpulse.air_quality', aqi_payload):
                logging.warning(f"Simulating Sensor Outage for {sensor_id}: Injected Null AQI routed to DLQ")

            # --- Stream 3: Smart Meters ---
            ward_id = random.choice(wards)
            meter_payload = {
                "meter_id": f"MTR-{random.randint(10000, 99999)}", 
                "ward_id": ward_id,
                "kwh_reading": round(random.uniform(0.5, 4.5), 2), 
                "voltage": random.randint(210, 245),
                "power_factor": round(random.uniform(0.82, 0.99), 2), 
                "timestamp": timestamp
            }
            validate_and_route('urbanpulse.smart_meters', meter_payload)
            
            # --- Stream 4: Traffic Signals ---
            jnc_id = random.choice(junctions)
            traffic_payload = {
                "junction_id": jnc_id, 
                "zone": "Zone-Core", 
                "vehicle_count": random.randint(10, 120),
                "avg_wait_sec": random.choice([45, 60, 90, 200]), 
                "signal_phase": "RED", 
                "timestamp": timestamp
            }
            validate_and_route('urbanpulse.traffic_signals', traffic_payload)

            producer.poll(0)
            time.sleep(0.3)
            
    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
    finally:
        producer.flush()

if __name__ == "__main__":
    simulate_urban_pulse()