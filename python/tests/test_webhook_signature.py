import hashlib
import hmac

from mal_pipeline.ingest.aml_webhook_receiver import verify_signature


def _sign(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def test_verify_signature_accepts_valid():
    secret = "vendor-secret"
    body = b'{"event_id":"e1"}'
    sig = _sign(secret, body)
    assert verify_signature(body=body, signature_header=sig, secret=secret) is True


def test_verify_signature_rejects_tampering():
    secret = "vendor-secret"
    body = b'{"event_id":"e1"}'
    sig = _sign(secret, body)
    assert verify_signature(body=b'{"event_id":"e2"}', signature_header=sig, secret=secret) is False


def test_verify_signature_missing_header():
    assert verify_signature(body=b"x", signature_header=None, secret="s") is False
