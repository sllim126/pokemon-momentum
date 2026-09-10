"""PSA certification validation, normalization, caching policy, and HTTP client."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os

import requests
from fastapi import HTTPException


PSA_API_BASE_URL = "https://api.psacard.com/publicapi"
PSA_CERT_CACHE_TTL_DAYS = 30


def psa_access_token() -> str:
    """Return the configured PSA API bearer token without exposing it to callers."""
    return os.getenv("POKEMON_MOMENTUM_PSA_ACCESS_TOKEN", "").strip()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso8601(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def validate_psa_cert_number(cert_number: str) -> str:
    """Normalize a PSA cert number and reject unsafe or implausible values."""
    normalized = str(cert_number or "").strip()
    if not normalized.isdigit() or len(normalized) < 4 or len(normalized) > 20:
        raise HTTPException(status_code=400, detail="PSA cert number must be 4-20 digits.")
    return normalized


def psa_cache_ttl() -> timedelta:
    raw_value = str(os.getenv("POKEMON_MOMENTUM_PSA_CACHE_TTL_DAYS", PSA_CERT_CACHE_TTL_DAYS)).strip()
    try:
        ttl_days = int(raw_value)
    except ValueError:
        ttl_days = PSA_CERT_CACHE_TTL_DAYS
    return timedelta(days=max(ttl_days, 1))


def psa_cache_is_fresh(row: dict | None, *, now: datetime | None = None) -> bool:
    """Return whether a stored certification is still inside the configured TTL."""
    if not row:
        return False
    checked_at = _parse_iso8601(row.get("last_checked_at"))
    if checked_at is None:
        return False
    current = now or utcnow()
    if checked_at.tzinfo is None:
        checked_at = checked_at.replace(tzinfo=timezone.utc)
    return current - checked_at < psa_cache_ttl()


def _payload_value(payload: dict, *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _nested_dict(payload: dict, key: str) -> dict:
    value = payload.get(key)
    return value if isinstance(value, dict) else {}


def normalize_psa_lookup(cert_number: str, payload: dict) -> dict:
    """Convert PSA's nested and legacy response shapes into one stable contract."""
    server_message = str(payload.get("ServerMessage") or "").strip()
    message_lower = server_message.lower()
    psa_cert = _nested_dict(payload, "PSACert")
    dna_cert = _nested_dict(payload, "DNACert")
    flattened = {**payload, **psa_cert}
    card_name = _payload_value(flattened, "Subject", "CardName", "Card", "Name", "ItemDescription")
    set_name = _payload_value(flattened, "SetName", "Brand", "Set")
    grade = _payload_value(flattened, "Grade", "CardGrade", "NumericGrade")
    year = _payload_value(flattened, "Year")
    brand = _payload_value(flattened, "Brand")
    subject = _payload_value(flattened, "Subject")
    card_number = _payload_value(flattened, "CardNumber", "Number")
    variety = _payload_value(flattened, "Variety")
    grade_description = _payload_value(flattened, "GradeDescription")
    item_status = _payload_value(flattened, "ItemStatus")
    certification_type = _payload_value(payload, "CertificationType")
    has_cert_data = bool(psa_cert or dna_cert or any([card_name, set_name, grade, year, brand]))
    is_valid_request = bool(payload.get("IsValidRequest")) if "IsValidRequest" in payload else has_cert_data

    if not is_valid_request:
        lookup_status = "invalid_cert"
    elif "no data" in message_lower:
        lookup_status = "not_found"
    elif message_lower == "request successful" or has_cert_data:
        lookup_status = "success"
    else:
        lookup_status = "unknown"

    card = None
    if lookup_status == "success":
        card = {
            "cert_number": cert_number,
            "card_name": card_name,
            "set_name": set_name,
            "grade": grade,
            "year": year,
            "brand": brand,
            "subject": subject,
            "card_number": card_number,
            "variety": variety,
            "grade_description": grade_description,
            "item_status": item_status,
            "certification_type": certification_type,
        }

    return {
        "cert_number": cert_number,
        "lookup_status": lookup_status,
        "is_valid_request": is_valid_request,
        "server_message": server_message,
        "card": card,
        "normalized_card_name": card_name,
        "normalized_set_name": set_name,
        "normalized_grade": grade,
        "raw": payload,
    }


def psa_lookup_response(row: dict, *, source: str) -> dict:
    """Build the public response contract from a stored certification row."""
    normalized = normalize_psa_lookup(str(row.get("cert_number") or ""), row.get("raw_response_json") or {})
    return {
        "cert_number": normalized["cert_number"],
        "lookup_status": row.get("lookup_status") or normalized["lookup_status"],
        "is_valid_request": bool(row.get("is_valid_request")),
        "server_message": row.get("server_message") or normalized["server_message"],
        "card": normalized["card"],
        "raw": row.get("raw_response_json") or {},
        "source": source,
        "cached": source == "cache",
        "last_checked_at": row.get("last_checked_at"),
    }


def fetch_psa_cert_from_upstream(cert_number: str) -> dict:
    """Fetch one certification from PSA and translate transport failures to API errors."""
    token = psa_access_token()
    if not token:
        raise HTTPException(status_code=503, detail="PSA integration is not configured.")

    url = f"{PSA_API_BASE_URL}/cert/GetByCertNumber/{cert_number}"
    try:
        response = requests.get(
            url,
            headers={
                "Authorization": f"bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=12,
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"PSA lookup failed: {exc}") from exc

    if response.status_code >= 500:
        raise HTTPException(status_code=502, detail="PSA lookup failed upstream.")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"PSA lookup returned unexpected status {response.status_code}.")
    if response.status_code == 204:
        return {"IsValidRequest": False, "ServerMessage": "No data found"}

    try:
        payload = response.json() if response.content else {}
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="PSA lookup returned invalid JSON.") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail="PSA lookup returned an unexpected payload.")
    return payload
