import base64
import cbor2
import hashlib
import json
import os

from config import defaults
from ecdsa import NIST256p, VerifyingKey
from ecdsa.util import sigdecode_der
from typing import Any

__all__ = [
    "b64url_encode",
    "b64url_decode",
    "generate_challenge",
    "generate_registration_options",
    "verify_registration_response",
    "generate_authentication_options",
    "verify_authentication_response",
    "get_challenge_from_attestation",
]

FLAG_UP = 0x01  # User Presence
FLAG_UV = 0x04  # User Verification
FLAG_AT = 0x40  # Attested credential data present


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


def b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _validate_client_data(
    cdata_raw: bytes, expected_type: str, expected_challenge: str
) -> None:
    """Validate client data JSON for WebAuthn operations."""
    cdata = json.loads(cdata_raw)
    if cdata.get("type") != expected_type:
        raise ValueError("Invalid clientData type")
    if cdata.get("challenge") != expected_challenge:
        raise ValueError("Mismatched challenge")
    if cdata.get("origin") != f"https://{defaults.HOSTNAME}":
        raise ValueError("Invalid origin")


def _validate_auth_data_header(auth_data: bytes, required_flags: int) -> int:
    """Validate authenticator data header (RP ID, flags) and return sign count."""
    if auth_data[:32] != hashlib.sha256(defaults.HOSTNAME.encode()).digest():
        raise ValueError("RP ID hash mismatch")
    flags = auth_data[32]
    if (flags & required_flags) != required_flags:
        missing = []
        if not (flags & FLAG_UP):
            missing.append("UP")
        if not (flags & FLAG_UV):
            missing.append("UV")
        if (required_flags & FLAG_AT) and not (flags & FLAG_AT):
            missing.append("AT")
        raise ValueError(f"Required flag(s) not set: {', '.join(missing)}")
    return int.from_bytes(auth_data[33:37], "big")


def _build_credential_list(cred_ids: list[bytes]) -> list[dict[str, str]]:
    """Build allowCredentials/excludeCredentials list."""
    return [{"type": "public-key", "id": b64url_encode(cid)} for cid in cred_ids]


def generate_challenge(length: int = 32) -> str:
    return b64url_encode(os.urandom(length))


def generate_registration_options(
    user_id: str,
    user_name: str,
    user_display_name: str | None = None,
    exclude_credentials: list[bytes | None] = [],
) -> dict:
    challenge = generate_challenge()
    opts: dict[str, Any] = {
        "challenge": challenge,
        "rp": {"id": defaults.HOSTNAME, "name": defaults.HOSTNAME},
        "user": {
            "id": b64url_encode(user_id.encode()),
            "name": user_name,
            "displayName": user_display_name or user_name,
        },
        "pubKeyCredParams": [{"type": "public-key", "alg": -7}],  # ES256
        "timeout": defaults.WEBAUTHN_CHALLENGE_TIMEOUT * 1000,
        "attestation": "none",
        "authenticatorSelection": {
            "userVerification": "required",
            "residentKey": "required",
        },
    }

    if exclude_list := [c for c in exclude_credentials if c]:
        opts["excludeCredentials"] = _build_credential_list(exclude_list)

    return {"challenge": challenge, "options": opts}


def get_challenge_from_attestation(attestation_response: dict) -> str | None:
    cdata_raw = b64url_decode(attestation_response["response"]["clientDataJSON"])
    cdata = json.loads(cdata_raw)
    return cdata.get("challenge")


def verify_registration_response(
    attestation_response: dict,
    expected_challenge: str,
) -> dict[str, Any]:
    cdata_raw = b64url_decode(attestation_response["response"]["clientDataJSON"])
    _validate_client_data(cdata_raw, "webauthn.create", expected_challenge)

    att_obj = cbor2.loads(
        b64url_decode(attestation_response["response"]["attestationObject"])
    )
    auth_data: bytes = att_obj["authData"]
    sign_count = _validate_auth_data_header(auth_data, FLAG_UP | FLAG_UV | FLAG_AT)

    # Extract credential data
    att_data = auth_data[37:]
    cred_id_len = int.from_bytes(att_data[16:18], "big")
    cred_id = att_data[18 : 18 + cred_id_len]
    cose = cbor2.loads(att_data[18 + cred_id_len :])

    # Extract public key from COSE (ES256: x at -2, y at -3)
    x, y = cose.get(-2), cose.get(-3)
    if not (isinstance(x, bytes) and isinstance(y, bytes)):
        raise ValueError("Invalid COSE EC2 key (x/y)")

    return {
        "credential_id": cred_id,
        "public_key_pem": VerifyingKey.from_string(x + y, curve=NIST256p)
        .to_pem()
        .decode(),
        "sign_count": sign_count,
    }


def generate_authentication_options(
    allowed_credentials: list[bytes] | None = None,
) -> dict[str, Any]:
    challenge = generate_challenge()
    opts = {
        "challenge": challenge,
        "rpId": defaults.HOSTNAME,
        "timeout": defaults.WEBAUTHN_CHALLENGE_TIMEOUT * 1000,
        "userVerification": "required",
        "allowCredentials": _build_credential_list(allowed_credentials or []),
    }
    return {"challenge": challenge, "options": opts}


def verify_authentication_response(
    assertion_response: dict,
    expected_challenge: str,
    public_key_pem: str,
    prev_sign_count: int = 0,
) -> dict[str, Any]:
    cdata_raw = b64url_decode(assertion_response["response"]["clientDataJSON"])
    _validate_client_data(cdata_raw, "webauthn.get", expected_challenge)

    auth_data = b64url_decode(assertion_response["response"]["authenticatorData"])
    sign_count = _validate_auth_data_header(auth_data, FLAG_UP | FLAG_UV)

    # Verify signature
    sig = b64url_decode(assertion_response["response"]["signature"])
    client_hash = hashlib.sha256(cdata_raw).digest()
    if not VerifyingKey.from_pem(public_key_pem).verify(
        sig, auth_data + client_hash, hashfunc=hashlib.sha256, sigdecode=sigdecode_der
    ):
        raise ValueError("Signature verification failed")

    # Check sign count
    counter_supported = sign_count != 0
    warning = None
    if counter_supported and prev_sign_count != 0 and sign_count <= prev_sign_count:
        warning = "non_increasing_sign_count"
        sign_count = prev_sign_count

    user_handle_b64 = assertion_response["response"].get("userHandle")
    return {
        "sign_count": sign_count,
        "counter_supported": counter_supported,
        "warning": warning,
        "user_handle": b64url_decode(user_handle_b64) if user_handle_b64 else None,
    }
