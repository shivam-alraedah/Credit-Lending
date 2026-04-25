from mal_pipeline.decision.policy import InputAvailability, derive_policy


class _FakeConn:
    """Minimal stand-in for psycopg connection: routes SQL through a lookup."""

    def __init__(self, caps=None, usage=None):
        self._caps = caps or {}
        self._usage = usage or {}
        self._seen = []

    class _Cursor:
        def __init__(self, parent, out):
            self.parent = parent
            self.out = out

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params):
            self.parent._seen.append((sql, params))
            if "from gold.degraded_policy_cap" in sql:
                self._rows = self.parent._caps.get((params["p"], params["c"]), [])
            elif "from gold.degraded_policy_daily_usage" in sql:
                self._rows = self.parent._usage.get((params["d"], params["p"], params["c"]), [])
            else:
                self._rows = []

        def fetchall(self):
            return self._rows

    def cursor(self):
        return _FakeConn._Cursor(self, None)


def test_full_inputs_allowed_when_everything_present():
    conn = _FakeConn()
    verdict = derive_policy(
        conn,
        product_id="bnpl_3m",
        product_type="bnpl",
        requested_amount_aed=1000.0,
        availability=InputAvailability(
            bureau_present=True, bureau_fresh=True, fraud_present=True, aml_present=True, internal_present=True
        ),
    )
    assert verdict.policy_code == "full_inputs"
    assert verdict.allowed is True


def test_pf_hard_blocks_without_bureau():
    conn = _FakeConn()
    verdict = derive_policy(
        conn,
        product_id="pf_murabaha_12m",
        product_type="personal_finance",
        requested_amount_aed=25_000.0,
        availability=InputAvailability(
            bureau_present=False, bureau_fresh=False, fraud_present=True, aml_present=True, internal_present=True
        ),
    )
    assert verdict.allowed is False
    assert verdict.policy_code == "required_bureau"


def test_bnpl_degraded_within_caps():
    caps_row = (5, 5000.0, 1500.0)
    usage_row = (2, 2000.0)
    conn = _FakeConn(
        caps={("bnpl_3m", "degraded_no_bureau"): [caps_row]},
        usage={},
    )
    # no usage entry means zero usage used
    _ = usage_row  # not used; keep for clarity
    verdict = derive_policy(
        conn,
        product_id="bnpl_3m",
        product_type="bnpl",
        requested_amount_aed=1000.0,
        availability=InputAvailability(
            bureau_present=False, bureau_fresh=False, fraud_present=True, aml_present=True, internal_present=True
        ),
    )
    assert verdict.policy_code == "degraded_no_bureau"
    assert verdict.allowed is True
    assert verdict.requires_reassessment is True


def test_bnpl_degraded_exceeds_per_decision_max():
    caps_row = (5, 5000.0, 1500.0)
    conn = _FakeConn(caps={("bnpl_3m", "degraded_no_bureau"): [caps_row]})
    verdict = derive_policy(
        conn,
        product_id="bnpl_3m",
        product_type="bnpl",
        requested_amount_aed=2000.0,
        availability=InputAvailability(
            bureau_present=False, bureau_fresh=False, fraud_present=True, aml_present=True, internal_present=True
        ),
    )
    assert verdict.allowed is False
    assert verdict.reason == "exceeds_per_decision_max"


def test_aml_missing_blocks_all():
    conn = _FakeConn()
    verdict = derive_policy(
        conn,
        product_id="bnpl_3m",
        product_type="bnpl",
        requested_amount_aed=100.0,
        availability=InputAvailability(
            bureau_present=True, bureau_fresh=True, fraud_present=True, aml_present=False, internal_present=True
        ),
    )
    assert verdict.policy_code == "blocked"
    assert verdict.allowed is False
