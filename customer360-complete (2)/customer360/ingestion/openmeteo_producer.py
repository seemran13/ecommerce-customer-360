#!/usr/bin/env python3
"""Open-Meteo current weather for major cities -> Kafka `weather` every 10 minutes (no API key)."""
import json, time, urllib.request
from producers_common import make_producer
CITIES = {"Hyderabad": (17.38, 78.48), "Mumbai": (19.07, 72.88), "Delhi": (28.61, 77.21), "Bengaluru": (12.97, 77.59),
          "Chennai": (13.08, 80.27), "Pune": (18.52, 73.86), "Kolkata": (22.57, 88.36)}
def main():
    p = make_producer()
    while True:
        for c, (la, lo) in CITIES.items():
            try:
                u = f"https://api.open-meteo.com/v1/forecast?latitude={la}&longitude={lo}&current=temperature_2m,precipitation,weather_code"
                d = json.load(urllib.request.urlopen(u, timeout=15))["current"]
                p.send("weather", key=c, value={"event_ts": d["time"] + ":00", "city": c, "temp_c": d["temperature_2m"],
                                                "precip_mm": d["precipitation"], "weather_code": d["weather_code"]})
            except Exception as e: print("weather err", c, e)
        p.flush(); time.sleep(600)
if __name__ == "__main__": main()
