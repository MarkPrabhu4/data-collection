import requests
import pandas as pd
import os
from datetime import datetime

CSV_PATH = "data/aqi_data.csv"

CITIES = {
    "delhi": (28.6139, 77.2090),
    "mumbai": (19.0760, 72.8777),
    "hyderabad": (17.3850, 78.4867),
    "bangalore": (12.9716, 77.5946),
    "kolkata": (22.5726, 88.3639),
    "chennai": (13.0827, 80.2707)
}

# ===============================
# AQI SUB-INDEX FUNCTIONS
# ===============================

def sub_index_pm25(c):
    if c <= 30: return c * 50 / 30
    elif c <= 60: return 50 + (c - 30) * 50 / 30
    elif c <= 90: return 100 + (c - 60) * 100 / 30
    elif c <= 120: return 200 + (c - 90) * 100 / 30
    elif c <= 250: return 300 + (c - 120) * 100 / 130
    else: return 500

def sub_index_pm10(c):
    if c <= 50: return c * 50 / 50
    elif c <= 100: return 50 + (c - 50) * 50 / 50
    elif c <= 250: return 100 + (c - 100) * 100 / 150
    elif c <= 350: return 200 + (c - 250) * 100 / 100
    elif c <= 430: return 300 + (c - 350) * 100 / 80
    else: return 500

def sub_index_no2(c):
    if c <= 40: return c * 50 / 40
    elif c <= 80: return 50 + (c - 40) * 50 / 40
    elif c <= 180: return 100 + (c - 80) * 100 / 100
    elif c <= 280: return 200 + (c - 180) * 100 / 100
    elif c <= 400: return 300 + (c - 280) * 100 / 120
    else: return 500

def sub_index_o3(c):
    if c <= 50: return c * 50 / 50
    elif c <= 100: return 50 + (c - 50) * 50 / 50
    elif c <= 168: return 100 + (c - 100) * 100 / 68
    elif c <= 208: return 200 + (c - 168) * 100 / 40
    elif c <= 748: return 300 + (c - 208) * 100 / 540
    else: return 500

def compute_aqi(pm25, pm10, no2, o3):
    return int(max(
        sub_index_pm25(pm25),
        sub_index_pm10(pm10),
        sub_index_no2(no2),
        sub_index_o3(o3)
    ))

# ===============================
# FETCH POLLUTION DATA
# ===============================

def fetch_pollution(lat, lon):
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm2_5,pm10,nitrogen_dioxide,carbon_monoxide,ozone"
    }

    data = requests.get(url, params=params).json()

    return {
        "pm25": data["hourly"]["pm2_5"][-1],
        "pm10": data["hourly"]["pm10"][-1],
        "no2": data["hourly"]["nitrogen_dioxide"][-1],
        "co": data["hourly"]["carbon_monoxide"][-1],
        "o3": data["hourly"]["ozone"][-1]
    }

# ===============================
# FETCH WEATHER DATA
# ===============================

def fetch_weather(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": True,
        "hourly": "relativehumidity_2m"
    }

    data = requests.get(url, params=params).json()

    return {
        "temperature": data["current_weather"]["temperature"],
        "wind_speed": data["current_weather"]["windspeed"],
        "humidity": data["hourly"]["relativehumidity_2m"][-1]
    }

# ===============================
# SAVE DATA
# ===============================

def save_row(row):
    df = pd.DataFrame([row])

    if not os.path.exists(CSV_PATH):
        df.to_csv(CSV_PATH, index=False)
    else:
        df.to_csv(CSV_PATH, mode='a', header=False, index=False)

# ===============================
# MAIN
# ===============================

def main():
    for city, (lat, lon) in CITIES.items():
        try:
            pollution = fetch_pollution(lat, lon)
            weather = fetch_weather(lat, lon)

            aqi = compute_aqi(
                pollution["pm25"],
                pollution["pm10"],
                pollution["no2"],
                pollution["o3"]
            )

            row = {
                "timestamp": datetime.utcnow(),
                "city": city,
                **pollution,
                **weather,
                "aqi": aqi
            }

            save_row(row)
            print(f"{city} → AQI: {aqi}")

        except Exception as e:
            print(f"Error for {city}: {e}")

if __name__ == "__main__":
    main()