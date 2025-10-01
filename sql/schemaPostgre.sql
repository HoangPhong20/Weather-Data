CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    time TIMESTAMP NOT NULL,
    temperature FLOAT,
    humidity FLOAT,
    wind_speed FLOAT
);