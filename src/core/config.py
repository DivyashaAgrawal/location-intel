"""Configuration and constants."""
from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

SERPER_API_KEY = os.getenv("SERPER_API_KEY", "")
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

INDIA_MAJOR_CITIES = {
    "Delhi": {"lat": 28.6139, "lng": 77.2090},
    "Mumbai": {"lat": 19.0760, "lng": 72.8777},
    "Bangalore": {"lat": 12.9716, "lng": 77.5946},
    "Chennai": {"lat": 13.0827, "lng": 80.2707},
    "Hyderabad": {"lat": 17.3850, "lng": 78.4867},
    "Kolkata": {"lat": 22.5726, "lng": 88.3639},
    "Pune": {"lat": 18.5204, "lng": 73.8567},
    "Ahmedabad": {"lat": 23.0225, "lng": 72.5714},
    "Jaipur": {"lat": 26.9124, "lng": 75.7873},
    "Lucknow": {"lat": 26.8467, "lng": 80.9462},
    "Chandigarh": {"lat": 30.7333, "lng": 76.7794},
    "Indore": {"lat": 22.7196, "lng": 75.8577},
    "Bhopal": {"lat": 23.2599, "lng": 77.4126},
    "Patna": {"lat": 25.6093, "lng": 85.1376},
    "Nagpur": {"lat": 21.1458, "lng": 79.0882},
    "Coimbatore": {"lat": 11.0168, "lng": 76.9558},
    "Kochi": {"lat": 9.9312, "lng": 76.2673},
    "Gurgaon": {"lat": 28.4595, "lng": 77.0266},
    "Noida": {"lat": 28.5355, "lng": 77.3910},
    "Surat": {"lat": 21.1702, "lng": 72.8311},
}

NOMINATIM_USER_AGENT = "location-intel-prototype/1.0"

# Hard guardrail. Queries projected to exceed this many Google Places
# enrichment calls are blocked with a "please narrow scope" message.
# 100 keeps us well inside the 1K free Enterprise tier and completes in
# under ~90s, and forces focused city-level analysis (the IC-prep workflow
# we actually optimise for).
MAX_ENRICHMENT_CALLS_PER_QUERY = 100

TIER_1_CITIES: list[str] = [
    "Mumbai", "Delhi", "Bangalore", "Chennai",
    "Hyderabad", "Pune", "Kolkata",
]
