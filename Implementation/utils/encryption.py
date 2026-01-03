import os
from cryptography.fernet import Fernet
from typing import Dict, Any


def get_encryption_key() -> bytes:
    key = os.getenv("ENCRYPTION_KEY")
    if not key:
        raise EnvironmentError("ENCRYPTION_KEY not set")
    return key.encode()


def encrypt_payload(payload: Dict[str, Any]) -> bytes:
    key = get_encryption_key()
    cipher = Fernet(key)
    import json

    return cipher.encrypt(json.dumps(payload).encode("utf-8"))


def decrypt_payload(token: bytes):
    key = get_encryption_key()
    cipher = Fernet(key)
    import json

    return json.loads(cipher.decrypt(token).decode("utf-8"))
