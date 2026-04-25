from mal_pipeline.common.hashing import sha256_bytes, sha256_json_canonical, sha256_text


def test_sha256_bytes_is_hex_and_stable():
    h = sha256_bytes(b"hello")
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)
    assert sha256_bytes(b"hello") == h


def test_sha256_text_matches_bytes():
    assert sha256_text("hello") == sha256_bytes(b"hello")


def test_sha256_json_canonical_order_independent():
    a = {"b": 1, "a": [1, {"y": 2, "x": 1}]}
    b = {"a": [1, {"x": 1, "y": 2}], "b": 1}
    assert sha256_json_canonical(a) == sha256_json_canonical(b)


def test_sha256_json_canonical_detects_change():
    a = {"a": 1}
    b = {"a": 2}
    assert sha256_json_canonical(a) != sha256_json_canonical(b)
