from mal_pipeline.identity.normalize import normalize_email, normalize_name, normalize_phone_e164


def test_email_lowercased_and_stripped():
    assert normalize_email("  John.Doe@Example.COM ") == "john.doe@example.com"
    assert normalize_email(None) is None
    assert normalize_email("") is None


def test_phone_keeps_plus_and_digits_only():
    assert normalize_phone_e164("+971 (50) 123-4567") == "+971501234567"
    assert normalize_phone_e164("abc") is None


def test_name_normalized_for_case_and_accents():
    assert normalize_name("  Áhmed  Alí ") == "ahmed ali"
    assert normalize_name(None) is None
    # Whitespace collapse
    assert normalize_name("Ali    Hassan") == "ali hassan"
