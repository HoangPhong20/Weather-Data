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
        print(f"---------Extracted data from {city},{country}: {data}----------")

        producer.send(topic, value=data).get(timeout=10)
        print(f"-----------Sending data from {city},{country} to {topic}-----------")

    except Exception as e:
        print(f"---------Error occurred: {e}------------")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    request_timeout_ms=10000,
    metadata_max_age_ms=30000,
    max_block_ms=15000
)
# Lấy danh sách thành phố
vn_cities = get_cities("VN")
laos_cities = get_cities("LA")

# Gửi dữ liệu thời tiết đến  cho topic vietnam
for city_name in vn_cities["name"]:
    send_weather(producer, city_name, "VN", "vietnam")

# Gửi weather cho Lào
for city_name in laos_cities["name"]:
    send_weather(producer, city_name, "LA", "laos")

producer.flush()
producer.close()
