-- 1. Create the database (schema) if it doesn't exist
CREATE DATABASE IF NOT EXISTS city_transport_db;

-- 2. Select the database to start using it
USE city_transport_db;

-- 3. Create relational DB to store final data
CREATE TABLE route_summary (
    route_id INT AUTO_INCREMENT PRIMARY KEY,  -- It is good practice to add a unique ID
    route_name VARCHAR(255),                  -- Text string for names
    transport_type VARCHAR(50),               -- Text string for type
    avg_ridership FLOAT,                      -- Numbers with decimals
    avg_congestion FLOAT,                     -- Numbers with decimals
    avg_speed FLOAT                           -- Numbers with decimals
);

SELECT * FROM route_summary;