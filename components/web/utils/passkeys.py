import base64
import cbor2
import hashlib
import json
import os
from typing import Any

from config import defaults
from ecdsa import NIST256p, VerifyingKey
from ecdsa.util import sigdecode_der

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
            "id": b64url_encode(user_id.encode("utf-8")),
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

    if exclude_credentials:
        exclude_list = []
        for cred_id in exclude_credentials:
            if cred_id:
                exclude_list.append(
                    {"type": "public-key", "id": b64url_encode(cred_id)}
                )
        if exclude_list:
            opts["excludeCredentials"] = exclude_list

    return {"challenge": challenge, "options": opts}


def get_challenge_from_attestation(attestation_response: dict) -> str | None:
    cdata_raw = b64url_decode(attestation_response["response"]["clientDataJSON"])
    cdata = json.loads(cdata_raw)
    return cdata.get("challenge")


def verify_registration_response(
    attestation_response: dict,
    expected_challenge: str,
) -> dict[str, Any]:
    origin = f"https://{defaults.HOSTNAME}"
    rp_id = defaults.HOSTNAME

    cdata_raw = b64url_decode(attestation_response["response"]["clientDataJSON"])
    cdata = json.loads(cdata_raw)
    if cdata.get("type") != "webauthn.create":
        raise ValueError("Invalid clientData type")
    if cdata.get("challenge") != expected_challenge:
        raise ValueError("Mismatched challenge")
    if cdata.get("origin") != origin:
        raise ValueError("Invalid origin")

    att_obj = cbor2.loads(
        b64url_decode(attestation_response["response"]["attestationObject"])
    )
    auth_data: bytes = att_obj["authData"]

    if auth_data[:32] != hashlib.sha256(rp_id.encode("utf-8")).digest():
        raise ValueError("RP ID hash mismatch")

    flags = auth_data[32]
    if not (flags & FLAG_UP):
        raise ValueError("User presence (UP) flag not set")
    if not (flags & FLAG_UV):
        raise ValueError("User verification (UV) flag not set")
    if not (flags & FLAG_AT):
        raise ValueError("Attested credential data (AT) flag not set")

    sign_count = int.from_bytes(auth_data[33:37], "big")

    att_data = auth_data[37:]
    cred_id_len = int.from_bytes(att_data[16:18], "big")
    cred_id = att_data[18 : 18 + cred_id_len]
    cose_pub_key = att_data[18 + cred_id_len :]
    cose = cbor2.loads(cose_pub_key)  # ES256: x at -2, y at -3

    x = cose.get(-2)
    y = cose.get(-3)
    if not (isinstance(x, bytes) and isinstance(y, bytes)):
        raise ValueError("Invalid COSE EC2 key (x/y)")
    vk = VerifyingKey.from_string(x + y, curve=NIST256p)
    pem = vk.to_pem().decode("utf-8")

    return {
        "credential_id": cred_id,
        "public_key_pem": pem,
        "sign_count": sign_count,
    }


def generate_authentication_options(
    allowed_credentials: list[bytes] | None = None,
) -> dict[str, Any]:
    challenge = generate_challenge()
    allow: list[dict[str, str]] = []
    if allowed_credentials:
        for cid in allowed_credentials:
            allow.append({"type": "public-key", "id": b64url_encode(cid)})

    opts = {
        "challenge": challenge,
        "rpId": defaults.HOSTNAME,
        "timeout": defaults.WEBAUTHN_CHALLENGE_TIMEOUT * 1000,
        "userVerification": "required",
        "allowCredentials": allow,
    }
    return {"challenge": challenge, "options": opts}


def verify_authentication_response(
    assertion_response: dict,
    expected_challenge: str,
    public_key_pem: str,
    prev_sign_count: int = 0,
) -> dict[str, Any]:
    origin = f"https://{defaults.HOSTNAME}"
    rp_id = defaults.HOSTNAME

    cdata_raw = b64url_decode(assertion_response["response"]["clientDataJSON"])
    cdata = json.loads(cdata_raw)
    if cdata.get("type") != "webauthn.get":
        raise ValueError("Invalid clientData type")
    if cdata.get("challenge") != expected_challenge:
        raise ValueError("Mismatched challenge")
    if cdata.get("origin") != origin:
        raise ValueError("Invalid origin")

    auth_data = b64url_decode(assertion_response["response"]["authenticatorData"])
    if auth_data[:32] != hashlib.sha256(rp_id.encode("utf-8")).digest():
        raise ValueError("RP ID hash mismatch")
    flags = auth_data[32]
    if not (flags & FLAG_UP):
        raise ValueError("User presence (UP) flag not set")
    if not (flags & FLAG_UV):
        raise ValueError("User verification (UV) flag not set")
    sign_count = int.from_bytes(auth_data[33:37], "big")

    sig = b64url_decode(assertion_response["response"]["signature"])
    client_hash = hashlib.sha256(cdata_raw).digest()
    vk = VerifyingKey.from_pem(public_key_pem)
    if not vk.verify(
        sig, auth_data + client_hash, hashfunc=hashlib.sha256, sigdecode=sigdecode_der
    ):
        raise ValueError("Signature verification failed")

    user_handle_b64 = assertion_response["response"].get("userHandle")
    user_handle = b64url_decode(user_handle_b64) if user_handle_b64 else None

    warning: str | None = None
    counter_supported = sign_count != 0
    if counter_supported and prev_sign_count != 0 and sign_count <= prev_sign_count:
        warning = "non_increasing_sign_count"
        sign_count = prev_sign_count

    return {
        "sign_count": sign_count,
        "counter_supported": counter_supported,
        "warning": warning,
        "user_handle": user_handle,
    }
