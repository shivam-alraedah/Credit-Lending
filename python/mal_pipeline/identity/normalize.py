from __future__ import annotations

import re
import unicodedata


_MULTISPACE = re.compile(r"\s+")


def normalize_email(email: str | None) -> str | None:
    if not email:
        return None
    return email.strip().lower()


def normalize_phone_e164(phone: str | None) -> str | None:
    if not phone:
        return None
    # Assumes input is already close to E.164; keep digits and leading +
    phone = phone.strip()
    phone = re.sub(r"[^\d+]", "", phone)
    return phone or None


def normalize_name(name: str | None) -> str | None:
    if not name:
        return None
    # Minimal normalization: casefold + strip accents + collapse whitespace.
    # Production: add Arabic-specific normalization and transliteration strategy.
    s = unicodedata.normalize("NFKD", name).casefold()
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = _MULTISPACE.sub(" ", s).strip()
    return s or None

