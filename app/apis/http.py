# app/apis/http.py
import os
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("API_BASE_URL", "").rstrip("/")
TOKEN = os.getenv("API_TOKEN", "")

def _headers():
    headers = {"Accept": "application/json"}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    return headers

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=0.5, max=4))
def get(path: str, params: dict | None = None) -> dict:
    if not BASE_URL:
        raise RuntimeError("API_BASE_URL não configurado na .env")
    
    url = f"{BASE_URL}/{path.lstrip('/')}"  # Concatena a URL base com o path específico
    with httpx.Client(timeout=10) as client:
        response = client.get(url, headers=_headers(), params=params)
        response.raise_for_status()
        return response.json()
