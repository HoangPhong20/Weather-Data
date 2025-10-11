from kafka import KafkaProducer
import json
import pandas as pd
import requests
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("Producer")

# Load biến môi trường
load_dotenv()
API_KEY = os.getenv("API_KEY")
API_URL = os.getenv("API_URL")
JSON_PATH = os.getenv("json_path")

def get_cities(country_code: str) -> pd.DataFrame:
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    cities = [city for city in data if city.get("country") == country_code]
    return pd.DataFrame(cities)

def send_weather(producer: KafkaProducer, city: str, country: str, topic: str):
    params = {
        "q": f"{city},{country}",
        "appid": API_KEY,
        "lang": "en"
    }
    try:
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Extracted weather data from {city},{country}")
        producer.send(topic, value=data)
        logger.info(f"Sent weather data of {city},{country} → topic: {topic}")
    except Exception as e:
        logger.error(f"Error sending {city},{country}: {e}")

def process_region(country_codes, topic_name):
    for code in country_codes:
        try:
            cities = get_cities(code)
            logger.info(f"Processing {len(cities)} cities in {code} → topic: {topic_name}")
            for city_name in cities["name"]:
                send_weather(producer, city_name, code, topic_name)
        except Exception as e:
            logger.error(f"Failed processing country {code}: {e}")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    request_timeout_ms=10000,
    linger_ms=500,
    batch_size=65536,
    metadata_max_age_ms=30000,
    max_block_ms=15000
)

Southeast_asia = {"VN", "LA", "TH", "KH", "MY", "SG", "PH", "ID"}
East_asia = {"CN", "JP", "KR", "TW", "HK", "MO"}

logger.info("🚀 Starting Weather Data Producer ...")
process_region(Southeast_asia, "southeast_asia")
process_region(East_asia, "east_asia")
producer.flush()
producer.close()
logger.info("✅ Finished sending all weather data.")
