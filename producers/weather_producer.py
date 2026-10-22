"""Open-Meteo current weather (no API key) -> weather_events, every 5 minutes, for the marketplace's key cities.
Weather is a contextual feature for demand/delivery-delay analysis."""
import json
import logging
import time
import urllib.request

from c360.config import settings as S
from producers.cart_event import iso_now
from producers.common import make_producer, send

CITIES = {"Hyderabad": (17.385, 78.4867), "Bengaluru": (12.9716, 77.5946), "Mumbai": (19.076, 72.8777),
          "Chennai": (13.0827, 80.2707), "Delhi": (28.6139, 77.209), "Pune": (18.5204, 73.8567)}


def fetch(city, lat, lon):
    url = (f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
           "&current=temperature_2m,precipitation,wind_speed_10m,weather_code")
    with urllib.request.urlopen(url, timeout=10) as r:
        cur = json.load(r)["current"]
    return {"event_ts": iso_now(), "city": city, "temperature_c": cur["temperature_2m"],
            "precipitation_mm": cur["precipitation"], "wind_kmh": cur["wind_speed_10m"],
            "weather_code": cur["weather_code"], "source": "open-meteo"}


def main(interval=300):
    p = make_producer("weather")
    while True:
        for city, (lat, lon) in CITIES.items():
            try:
                send(p, S.topic_weather, city, fetch(city, lat, lon))
            except Exception as e:
                logging.warning("weather fetch failed for %s: %s", city, e)
        p.flush(10)
        time.sleep(interval)


if __name__ == "__main__":
    main()
