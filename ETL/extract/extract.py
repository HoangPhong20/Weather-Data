from kafka import KafkaProducer
import json
import pandas as pd
import requests
from dotenv import load_dotenv
import os

# Load biến môi trường
load_dotenv()
API_KEY = os.getenv("API_KEY")
API_URL = os.getenv("API_URL")
JSON_PATH = os.getenv("json_path")


def get_cities(country_code: str) -> pd.DataFrame:
    """Đọc file JSON và lấy danh sách city theo country_code"""
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    cities = [city for city in data if city.get("country") == country_code]
    return pd.DataFrame(cities)


def send_weather(producer: KafkaProducer, city: str, country: str, topic: str):
    """Gọi API thời tiết và gửi dữ liệu vào Kafka"""
    params = {
        "q": f"{city},{country}",
        "appid": API_KEY,
        "lang": "en"
    }

    try:
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        print(f"✅ Dữ liệu API cho {city},{country}: {data}")

        producer.send(topic, value=data).get(timeout=10)
        print(f"📤 Sent {city},{country} to topic {topic}")

    except Exception as e:
        print(f"❌ Failed {city},{country}: {e}")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    request_timeout_ms=10000,
    metadata_max_age_ms=30000,
    max_block_ms=15000
)

# Lấy city VN & Lào
vn_cities = get_cities("VN")
laos_cities = get_cities("LA")

print("VN Cities:", vn_cities)
print("LAOS Cities:", laos_cities)

# Gửi weather cho Viet Nam
for city_name in vn_cities["name"]:
    send_weather(producer, city_name, "VN", "vietnam")

# Gửi weather cho Lào
for city_name in laos_cities["name"]:
    send_weather(producer, city_name, "LA", "laos")

producer.flush()
producer.close()
