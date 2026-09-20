import requests
import psycopg2
from datetime import datetime, timezone

# 10 villes françaises avec leurs coordonnées GPS
VILLES = [
    {"nom": "Paris", "lat": 48.8566, "lon": 2.3522},
    {"nom": "Marseille", "lat": 43.2965, "lon": 5.3698},
    {"nom": "Lyon", "lat": 45.7640, "lon": 4.8357},
    {"nom": "Toulouse", "lat": 43.6047, "lon": 1.4442},
    {"nom": "Nice", "lat": 43.7102, "lon": 7.2620},
    {"nom": "Nantes", "lat": 47.2184, "lon": -1.5536},
    {"nom": "Strasbourg", "lat": 48.5734, "lon": 7.7521},
    {"nom": "Montpellier", "lat": 43.6108, "lon": 3.8767},
    {"nom": "Bordeaux", "lat": 44.8378, "lon": -0.5792},
    {"nom": "Lille", "lat": 50.6292, "lon": 3.0573},
]

import os

DB_CONFIG = {
    "host": os.getenv("METEO_DB_HOST", "localhost"),
    "port": int(os.getenv("METEO_DB_PORT", "5433")),
    "dbname": os.getenv("METEO_DB_NAME", "meteo_db"),
    "user": os.getenv("METEO_DB_USER", "dbt_user"),
    "password": os.getenv("METEO_DB_PASSWORD", "dbt_password"),
}

def fetch_weather(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
        "timezone": "auto"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_weather (
                id SERIAL PRIMARY KEY,
                ville VARCHAR(50),
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                humidite FLOAT,
                vitesse_vent FLOAT,
                code_meteo INT,
                extraction_timestamp TIMESTAMP
            );
        """)
    conn.commit()

def insert_weather_data(conn, ville, data):
    current = data["current"]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw_weather 
            (ville, latitude, longitude, temperature, humidite, vitesse_vent, code_meteo, extraction_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            ville["nom"],
            ville["lat"],
            ville["lon"],
            current["temperature_2m"],
            current["relative_humidity_2m"],
            current["wind_speed_10m"],
            current["weather_code"],
            datetime.now(timezone.utc)
        ))
    conn.commit()

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    create_table_if_not_exists(conn)

    for ville in VILLES:
        try:
            data = fetch_weather(ville["lat"], ville["lon"])
            insert_weather_data(conn, ville, data)
            print(f"✓ {ville['nom']} : {data['current']['temperature_2m']}°C")
        except Exception as e:
            print(f"✗ Erreur pour {ville['nom']} : {e}")

    conn.close()
    print("\nExtraction terminée.")

if __name__ == "__main__":
    main()