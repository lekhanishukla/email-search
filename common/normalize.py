import re
import hashlib

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def normalize_email(email: str) -> str:
    return email.strip().lower()

def is_valid_email(email: str) -> bool:
    return bool(EMAIL_RE.match(email))

def split_email(email: str) -> tuple[str, str]:
    if "@" not in email:
        return "", ""
    user, domain = email.split("@", 1)
    return user, domain

def stable_id(email: str) -> str:
    return hashlib.sha1(email.encode("utf-8")).hexdigest()
