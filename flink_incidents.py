

import json
import logging
import math
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [FLINK ENGINE] - %(message)s')

KAFKA_BOOTSTRAP = 'localhost:9092'
INPUT_TOPICS = ['urbanpulse.air_quality', 'urbanpulse.traffic_signals', 'urbanpulse.bus_gps']
OUTPUT_TOPIC = 'urbanpulse.incidents'

# Producer setup for emitting alerts
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

def emit_incident(incident_type, entity_id, zone, description, payload):
    alert = {
        "incident_type": incident_type,
        "entity_id": entity_id,
        "zone": zone,
        "description": description,
        "payload": payload,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    producer.produce(
        topic=OUTPUT_TOPIC,
        key=str(entity_id).encode('utf-8'),
        value=json.dumps(alert).encode('utf-8')
    )
    producer.flush()
    logging.warning(f"🚨 INCIDENT DETECTED [{incident_type}]: {description}")

def haversine_distance_meters(lat1, lon1, lat2, lon2):
    """Calculates distance between two geo-points in meters."""
    R = 6371000  # Radius of Earth in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2.0)**2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c

class UrbanPulseIncidentEngine:
    def __init__(self):
        # Keyed state objects
        self.traffic_state = {}  # {junction_id: consecutive_high_wait_count}
        self.bus_state = {}      # {route_id: {bus_id: (lat, lon, timestamp)}}

    def process_air_quality(self, data):
        """Pattern 1: AQI Emergency (> 300)"""
        aqi = data.get("aqi")
        sensor_id = data.get("sensor_id")
        zone = data.get("zone", "UNKNOWN")

        if aqi is not None and aqi > 300:
            emit_incident(
                incident_type="AQI_EMERGENCY",
                entity_id=sensor_id,
                zone=zone,
                description=f"Hazardous AQI breach detected: {aqi} (Threshold > 300)",
                payload=data
            )

    def process_traffic_signals(self, data):
        """Pattern 2: Traffic Gridlock (3 consecutive cycles > 180s)"""
        jnc_id = data.get("junction_id")
        zone = data.get("zone", "Zone-Core")
        avg_wait = data.get("avg_wait_sec", 0)

        # Update keyed state for junction
        current_count = self.traffic_state.get(jnc_id, 0)
        
        if avg_wait > 180:
            current_count += 1
            self.traffic_state[jnc_id] = current_count
            if current_count >= 3:
                emit_incident(
                    incident_type="TRAFFIC_GRIDLOCK",
                    entity_id=jnc_id,
                    zone=zone,
                    description=f"Junction {jnc_id} gridlock: wait time > 180s for {current_count} consecutive cycles",
                    payload=data
                )
        else:
            self.traffic_state[jnc_id] = 0  # Reset state on normal signal cycle

    def process_bus_gps(self, data):
        """Pattern 3: Bus Bunching (< 200m distance on same route)"""
        route_id = data.get("route_id")
        bus_id = data.get("bus_id")
        lat = data.get("lat")
        lon = data.get("lon")

        if not route_id or not bus_id or lat is None or lon is None:
            return

        # Initialize state for route if absent
        if route_id not in self.bus_state:
            self.bus_state[route_id] = {}

        # Update current position for this bus
        self.bus_state[route_id][bus_id] = (lat, lon)

        # Evaluate proximity against other buses on the same route
        route_buses = self.bus_state[route_id]
        for other_bus_id, (o_lat, o_lon) in route_buses.items():
            if other_bus_id != bus_id:
                dist_m = haversine_distance_meters(lat, lon, o_lat, o_lon)
                if dist_m <= 200.0:
                    emit_incident(
                        incident_type="BUS_BUNCHING",
                        entity_id=f"{bus_id}_{other_bus_id}",
                        zone=f"Route-{route_id}",
                        description=f"Bus bunching on route {route_id}: {bus_id} & {other_bus_id} are {round(dist_m, 1)}m apart",
                        payload={"bus_1": bus_id, "bus_2": other_bus_id, "route_id": route_id, "distance_meters": round(dist_m, 1)}
                    )

def run_engine():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'flink_incident_engine',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    consumer = Consumer(conf)
    consumer.subscribe(INPUT_TOPICS)

    engine = UrbanPulseIncidentEngine()
    logging.info("Incident Engine active. Subscribed to stream topics...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logging.error(f"Consumer error: {msg.error()}")
                continue

            topic = msg.topic()
            payload = json.loads(msg.value().decode('utf-8'))

            if topic == 'urbanpulse.air_quality':
                engine.process_air_quality(payload)
            elif topic == 'urbanpulse.traffic_signals':
                engine.process_traffic_signals(payload)
            elif topic == 'urbanpulse.bus_gps':
                engine.process_bus_gps(payload)

    except KeyboardInterrupt:
        logging.info("Shutting down Incident Engine...")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_engine()
