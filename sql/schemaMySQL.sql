CREATE TABLE IF NOT EXISTS weather_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    time DATETIME NOT NULL,
    temperature FLOAT,
    humidity FLOAT,
    wind_speed FLOAT
);
