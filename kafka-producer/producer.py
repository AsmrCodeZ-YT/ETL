#!/usr/bin/env python3
"""
Lightweight, production-minded Kafka Avro producer (single-file).
- Uses confluent_kafka's AvroSerializer + SerializingProducer.
- Schema Registry driven Avro schema (auto-register by default).
- Environment-configurable, graceful shutdown, structured logging,
  delivery callbacks, retry/backoff when the producer queue is full.
- Keep this file minimal and functional for easy Docker image builds.
"""

import os
import time
import uuid
import random
import logging
import signal
import sys
from typing import Dict, Any, Optional, Callable
from threading import Event

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from faker import Faker

fake = Faker()

# -----------------------
# Global control/state
# -----------------------
SHUTDOWN_EVENT = Event()
TOTAL_SENT = 0
TOTAL_FAILED = 0

# -----------------------
# Default Config
# -----------------------
DEFAULTS = {
    "BOOTSTRAP": "automq:9092",
    "SCHEMA_REGISTRY": "http://schema-registry:8081",
    "TOPIC": "user_events",
    "FLUSH_EVERY": "500",  # flush every N messages
    "SLEEP_MIN_SEC": "0.005",
    "SLEEP_MAX_SEC": "0.02",
    "LOG_LEVEL": "INFO",
    "AUTO_REGISTER_SCHEMAS": True # set to "false" to require pre-registered schemas
}


# -----------------------
# Schema (Avro)
# -----------------------
USER_EVENT_SCHEMA = """
{
  "type": "record",
  "name": "UserBehaviorEvent",
  "namespace": "com.example.analytics",
  "fields": [
    { "name": "event_id", "type": "string" },
    { "name": "user_id", "type": "string" },
    { "name": "action", "type": "string" },
    { "name": "pre_url", "type": "string" },
    { "name": "section", "type": "string" },
    { "name": "timestamp", "type": "long" },
    { "name": "device", "type": "string" },
    { "name": "browser", "type": "string" },
    { "name": "duration", "type": "int" }
  ]
}
"""

# -----------------------
# Utilities & Logging
# -----------------------
def setup_logging(level: str) -> None:
    """
    Configure root logger with a simple formatter.
    Args:
        level: logging level string (e.g., 'INFO', 'DEBUG')
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z"
    )
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(numeric_level)
    # Avoid duplicate handlers in some environments
    if not root.handlers:
        root.addHandler(handler)
    else:
        root.handlers = [handler]


# -----------------------
# Config loader
# -----------------------
def load_config() -> Dict[str, str]:
    """
    Load configuration from environment, applying defaults.
    Returns:
        A dict of config values (strings).
    Exits the process with a logged error if required configs are missing.
    """
    cfg = {}
    for k, v in DEFAULTS.items():
        cfg[k] = os.getenv(k, v)

    # Basic validation
    missing = []
    if not cfg["BOOTSTRAP"]:
        missing.append("BOOTSTRAP")
    if not cfg["SCHEMA_REGISTRY"]:
        missing.append("SCHEMA_REGISTRY")
    if missing:
        logging.error("Missing required environment variables: %s", missing)
        sys.exit(1)

    # Normalize booleans
    cfg["AUTO_REGISTER_SCHEMAS"] = cfg["AUTO_REGISTER_SCHEMAS"].lower() in ("true", "1", "yes")
    return cfg


# -----------------------
# Avro serializer helper
# -----------------------
def create_avro_serializer(schema_registry_url: str, schema_str: str, auto_register: bool) -> AvroSerializer:
    """
    Create an AvroSerializer using the provided schema registry URL and schema string.
    Args:
        schema_registry_url: URL of the Schema Registry.
        schema_str: Avro schema (string).
        auto_register: whether serializer should auto-register schemas.
    Returns:
        AvroSerializer instance.
    """
    client = SchemaRegistryClient({"url": schema_registry_url})

    # The to_dict function should convert our object to a plain dict - we already produce dicts
    def to_dict(obj: Dict[str, Any], ctx) -> Dict[str, Any]:
        # Here you could add validation/transformation
        return obj

    serializer_conf = {
        "auto.register.schemas": auto_register,
        # optionally: "use.latest.version": True
    }

    avro_ser = AvroSerializer(
        schema_registry_client=client,
        schema_str=schema_str,
        to_dict=to_dict,
        conf=serializer_conf
    )
    return avro_ser


# -----------------------
# Producer factory
# -----------------------
def create_producer(bootstrap: str, avro_serializer: AvroSerializer) -> SerializingProducer:
    """
    Create and configure a SerializingProducer with a StringSerializer for keys
    and the provided Avro serializer for values.
    """
    conf = {
        "bootstrap.servers": bootstrap,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
        # Tune producer configs here if needed:
        # "queue.buffering.max.messages": 100000,
        # "linger.ms": 5,
    }
    return SerializingProducer(conf)


# -----------------------
# Fake event generator
# -----------------------
ACTIONS = ["page_view", "add_to_cart", "remove_cart", "checkout", "search", "scroll"]
SECTIONS = ["home", "product_page", "profile", "checkout", "search", "category"]
DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]

def generate_event() -> Dict[str, Any]:
    """
    Generate a single fake user event dict according to the Avro schema.
    Returns:
        dict representing the event.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 5000)}",
        "action": random.choice(ACTIONS),
        "pre_url": random.choice(["/", "/product/123", "/profile", "/cart", "/search?q=laptop"]),
        "section": random.choice(SECTIONS),
        "timestamp": int(time.time() * 1000),
        "device": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "duration": random.randint(100, 5000)
    }


# -----------------------
# Delivery report callback
# -----------------------
def make_delivery_callback() -> Callable:
    """
    Returns a delivery callback which logs success or failure and updates global counters.
    """
    def delivery_report(err, msg):
        global TOTAL_SENT, TOTAL_FAILED
        if err is not None:
            TOTAL_FAILED += 1
            logging.error("Delivery failed for message key=%s: %s", getattr(msg, "key", None), err)
        else:
            TOTAL_SENT += 1
            # Optionally include partition/offset info
            logging.debug("Delivered message to %s [%d] at offset %s",
                          msg.topic(), msg.partition(), msg.offset())
    return delivery_report


# -----------------------
# Single-produce function with retry/backoff
# -----------------------
def produce_one(producer: SerializingProducer, topic: str, key: str, value: Dict[str, Any],
                callback: Callable, max_retries: int = 5) -> bool:
    """
    Produce one message to Kafka with limited retry/backoff for queue-full situations.
    Args:
        producer: SerializingProducer instance.
        topic: target topic.
        key: message key (string).
        value: message value (dict).
        callback: delivery callback function.
        max_retries: number of retries for transient errors like queue full.
    Returns:
        True if produce accepted (not necessarily delivered yet), False on unrecoverable error.
    """
    attempt = 0
    backoff = 0.1
    while attempt <= max_retries and not SHUTDOWN_EVENT.is_set():
        try:
            producer.produce(topic=topic, key=key, value=value, on_delivery=callback)
            return True
        except BufferError as be:
            # Local queue is full - backoff and retry
            logging.warning("Local producer queue is full (attempt %d/%d). Backing off %.3fs", attempt + 1, max_retries, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 1.0)  # cap backoff
            attempt += 1
        except Exception as ex:
            # Non-transient error - log and don't retry indefinitely
            logging.exception("Unexpected error while producing message: %s", ex)
            return False
    logging.error("Failed to enqueue message after %d attempts", max_retries)
    return False


# -----------------------
# Graceful shutdown handler
# -----------------------
def _signal_handler(signum, frame):
    """
    Set the shutdown flag on SIGINT/SIGTERM.
    """
    logging.info("Received signal %s, initiating graceful shutdown...", signum)
    SHUTDOWN_EVENT.set()


# -----------------------
# Main loop
# -----------------------
def run():
    """
    Main run function to boot producer and continuously generate/send events.
    """
    global TOTAL_SENT, TOTAL_FAILED

    cfg = load_config()
    setup_logging(cfg["LOG_LEVEL"])
    logger = logging.getLogger("producer")

    logger.info("Configuration: bootstrap=%s schema_registry=%s topic=%s",
                cfg["BOOTSTRAP"], cfg["SCHEMA_REGISTRY"], cfg["TOPIC"])

    # Create serializer & producer
    avro_ser = create_avro_serializer(cfg["SCHEMA_REGISTRY"], USER_EVENT_SCHEMA, cfg["AUTO_REGISTER_SCHEMAS"])
    producer = create_producer(cfg["BOOTSTRAP"], avro_ser)
    delivery_cb = make_delivery_callback()

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    flush_every = int(cfg["FLUSH_EVERY"])
    sleep_min = float(cfg["SLEEP_MIN_SEC"])
    sleep_max = float(cfg["SLEEP_MAX_SEC"])

    sent_since_flush = 0
    counter = 0
    last_report_time = time.time()

    logger.info("Producer starting main loop. Press Ctrl+C to exit.")

    try:
        while not SHUTDOWN_EVENT.is_set():
            event = generate_event()
            key = event["user_id"]

            accepted = produce_one(producer, cfg["TOPIC"], key, event, delivery_cb)
            if accepted:
                counter += 1
                sent_since_flush += 1

            # Periodic flush strategy
            if sent_since_flush >= flush_every:
                logger.debug("Flushing producer (sent_since_flush=%d)...", sent_since_flush)
                producer.flush()
                sent_since_flush = 0

            # Periodic health log (every 5 seconds)
            now = time.time()
            if now - last_report_time >= 5:
                rate = TOTAL_SENT / max((now - last_report_time), 1)
                logger.info("[HEALTH] total_sent=%d total_failed=%d last_interval_rate≈%.2f/s",
                            TOTAL_SENT, TOTAL_FAILED, rate)
                last_report_time = now
                # Reset interval counter if wanted (we keep totals here)

            # Non-busy wait with jitter to simulate realistic traffic and avoid tight loop
            time.sleep(random.uniform(sleep_min, sleep_max))

        # Shutdown requested - flush pending messages
        logger.info("Shutdown requested: flushing pending messages...")
        producer.flush()
        logger.info("Flush complete. Sent=%d Failed=%d", TOTAL_SENT, TOTAL_FAILED)

    except Exception as main_ex:
        logger.exception("Fatal error in producer main loop: %s", main_ex)
        try:
            producer.flush()
        except Exception:
            pass
        raise


if __name__ == "__main__":
    run()
