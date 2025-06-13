-- Schema
CREATE SCHEMA IF NOT EXISTS dw;

-- Dimensão: Localização
CREATE TABLE dw.dim_location (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100)
);

-- Dimensão: Tipo da cervejaria
CREATE TABLE dw.dim_brewery_type (
    brewery_type_id SERIAL PRIMARY KEY,
    brewery_type VARCHAR(50)
);

-- Dimensão: Nome da cervejaria
CREATE TABLE dw.dim_brewery_name (
    brewery_name_id SERIAL PRIMARY KEY,
    brewery_name VARCHAR(200) UNIQUE
);

-- Dimensão: Tempo
CREATE TABLE dw.dim_time (
    time_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    week_of_year INT
);

-- Tabela fato: Cervejarias
CREATE TABLE dw.fact_brewery (
    brewery_id SERIAL PRIMARY KEY,
    location_id INT REFERENCES dw.dim_location(location_id),
    brewery_type_id INT REFERENCES dw.dim_brewery_type(brewery_type_id),
    brewery_name_id INT REFERENCES dw.dim_brewery_name(brewery_name_id),
    time_id INT REFERENCES dw.dim_time(time_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);