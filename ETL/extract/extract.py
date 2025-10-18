import time
from kafka import KafkaProducer
import json
import requests
from dotenv import load_dotenv
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Lấy danh sách city
with open(JSON_PATH, "r", encoding="utf-8") as f:
    CITY_DATA = json.load(f)
def get_cities(country_code: str) -> list:
    return [c["name"] for c in CITY_DATA if c["country"] == country_code]


# Lấy dữ liệu từ API và gửi lên Kafka
def send_weather(producer: KafkaProducer, city: str, country: str, topic: str):
    params = {
        "q": f"{city},{country}",
        "appid": API_KEY,
        "lang": "en"
    }
    try:
        response = requests.get(API_URL, params=params, timeout=10)
        # Kiểm tra nếu vượt giới hạn (rate limit)
        if response.status_code == 429:
            logger.warning(" Rate limit reached! Waiting 10s before retry...")
            time.sleep(10)
            return
        response.raise_for_status()
        # Dữ liệu hợp lệ → gửi lên Kafka
        data = response.json()
        logger.info(f"Extracted weather data from {city},{country}")
        producer.send(topic, value=data)
        logger.info(f"Sent weather data of {city},{country} → topic: {topic}")
    except Exception as e:
        logger.error(f"Error sending {city},{country}: {e}")

# Xử lý khu vực
def process_region(country_codes, topic_name,max_workers=5):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [] # List lưu task đang chạy, dùng để chờ tất cả task hoàn thành
        for code in country_codes:
            try:
                cities = get_cities(code)
                logger.info(f"Processing {len(cities)} cities in {code} → topic: {topic_name}")
                for city_name in cities:
                    # Gửi mỗi thành phố là 1 tác vụ (thread)
                    future = executor.submit(send_weather, producer, city_name, code, topic_name)
                    futures.append(future)
            except Exception as e:
                logger.error(f"Failed processing country {code}: {e}")
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Task bị lỗi: {e}")
    logger.info(f"✅ Finished processing region: {topic_name}")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    request_timeout_ms=10000,
    linger_ms=100,
    batch_size=65536,
    acks=1
)

Southeast_asia = {"VN", "LA", "TH", "KH", "MY", "SG", "PH", "ID"}
East_asia = {"CN", "JP", "KR", "TW", "HK", "MO"}

logger.info("🚀 Starting Weather Data Producer ...")
with ThreadPoolExecutor(max_workers=2) as executor:
    f1 = executor.submit(process_region, Southeast_asia, "southeast_asia", 20)
    f2 = executor.submit(process_region, East_asia, "east_asia", 20)
    for f in as_completed([f1, f2]):
        try:
            f.result()
        except Exception as e:
            logger.error(f"Region task failed: {e}")
producer.flush()
producer.close()
logger.info("✅ Finished sending all weather data.")
