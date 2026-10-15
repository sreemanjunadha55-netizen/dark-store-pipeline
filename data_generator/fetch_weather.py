import time
import requests
import pandas as pd
from pathlib import Path

# ---------- Settings ----------
API_URL = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "2026-09-01"
END_DATE = "2026-09-30"
OUT = Path("data/raw")

# ---------- One coordinate per city ----------
stores = pd.read_csv(OUT / "stores.csv")
cities = stores.groupby("city", as_index=False).agg(
    latitude=("latitude", "mean"),
    longitude=("longitude", "mean"),
)

# ---------- Download ----------
frames = []
for c in cities.itertuples():
    params = {
        "latitude": round(c.latitude, 4),
        "longitude": round(c.longitude, 4),
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": "temperature_2m,precipitation",
        "timezone": "Asia/Kolkata",
    }

    for attempt in range(1, 4):
        try:
            response = requests.get(API_URL, params=params, timeout=60)
            response.raise_for_status()
            break
        except requests.RequestException as error:
            print(f"{c.city}: attempt {attempt} failed -> {error}")
            if attempt == 3:
                raise
            time.sleep(5)

    hourly = response.json()["hourly"]
    df = pd.DataFrame({
        "city": c.city,
        "weather_ts": pd.to_datetime(hourly["time"]),
        "temperature_c": hourly["temperature_2m"],
        "precipitation_mm": hourly["precipitation"],
    })
    frames.append(df)
    print(f"{c.city}: downloaded {len(df)} hourly rows")
    time.sleep(1)

weather = pd.concat(frames, ignore_index=True)

# ---------- Save ----------
days = weather["weather_ts"].dt.strftime("%Y-%m-%d")
for day, part in weather.groupby(days):
    folder = OUT / "weather" / f"date={day}"
    folder.mkdir(parents=True, exist_ok=True)
    part.to_parquet(folder / "weather.parquet", index=False)

# ---------- Summary ----------
print()
print("Total weather rows:", len(weather))
print("Missing values:", int(weather[["temperature_c", "precipitation_mm"]].isna().sum().sum()))
print("Rainy hours (> 0.1 mm) per city:")
print(weather[weather["precipitation_mm"] > 0.1].groupby("city").size())
print("Average temperature per city (C):")
print(weather.groupby("city")["temperature_c"].mean().round(1))


