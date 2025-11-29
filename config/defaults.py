import os

ACCEPT_LANGUAGES = ["en", "de"]
WEBAUTHN_CHALLENGE_TIMEOUT = 30  # seconds
AUTH_REQUEST_TIMEOUT = 300  # seconds
REGISTER_REQUEST_TIMEOUT = 300  # seconds
TABLE_PAGE_SIZE = 20
TRUSTED_PROXIES = ["127.0.0.1", "::1"]
TEMPLATES_AUTO_RELOAD = True
SEND_FILE_MAX_AGE_DEFAULT = 31536000
LOG_LEVEL = "DEBUG"
LOG_FILE_RETENTION = 3
LOG_FILE_ROTATION = 5  # MiB
PRESERVE_SESSION_KEYS = []
HYPERCORN_BIND = "162.55.49.111:443"
HOSTNAME = "gyst.debinux.de"
SECRET_KEY = os.getenv("SESSION_SECRET", "im-insecure")  # can be overridden by env var
TLS_CERTFILE = "system/certs/fullchain.pem"
TLS_KEYFILE = "system/certs/privkey.pem"
TLS_CA = "/etc/ssl/certs/ca-certificates.crt"
CLUSTER_PEERS = {
    "de.kerker.io": {
        "ip4": "2.58.53.49",
        "nat_ip4": "45.86.125.5",
    },
    "arm-2": {
        "ip4": "37.27.93.56",
        "ip6": "2a01:4f9:c012:7d9d::1",
    },
    "4th": {
        "ip4": "188.245.202.111",
        "ip6": "2a01:4f8:c17:9023::1",
    },
}
CLUSTER_SELF = {
    "name": "debian-4gb-nbg1-2",
    "ip4": "162.55.49.111",
    "ip6": "2a01:4f8:1c1c:72bd::1",
}
CLUSTER_CLI_BINDINGS = ["127.0.0.1", "::1"]
CLUSTER_PEERS_TIMEOUT = 1.25
OSM_EMAIL = "andre.peters@debinux.de"
CLAUDE_DEFAULT_MODEL = "claude-sonnet-4-5"
DISABLE_CLUSTER_QUORUM = False
