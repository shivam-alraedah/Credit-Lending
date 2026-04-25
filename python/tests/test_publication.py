import pytest

from mal_pipeline.dq.publication import (
    get_last_green_run,
    publish_if_green,
    set_publication_marker,
)


def test_get_last_green_run_returns_none_when_empty():
    from tests.conftest import FakeConn  # type: ignore

    conn = FakeConn()
    assert get_last_green_run(conn, "silver") is None


def test_get_last_green_run_returns_marker():
    from tests.conftest import FakeConn  # type: ignore
    from datetime import datetime, timezone

    ts = datetime(2025, 4, 22, 12, 0, tzinfo=timezone.utc)
    conn = FakeConn(responses={
        "from dq.publication_marker": [("silver", "a"*36, ts, "dq-runner", "ok")],
    })
    marker = get_last_green_run(conn, "silver")
    assert marker is not None
    assert marker.layer == "silver"
    assert marker.last_green_run_id == "a"*36
    assert marker.last_green_at == ts
    assert marker.updated_by == "dq-runner"


def test_get_last_green_run_invalid_layer_raises():
    from tests.conftest import FakeConn  # type: ignore

    conn = FakeConn()
    with pytest.raises(ValueError):
        get_last_green_run(conn, "fake")


def test_set_publication_marker_writes_upsert():
    from tests.conftest import FakeConn  # type: ignore

    conn = FakeConn()
    set_publication_marker(conn, layer="gold", dq_run_id="run-1", updated_by="dag")
    sqls = [s for s, _ in conn.executed]
    # ensure we issued the upsert
    assert any("insert into dq.publication_marker" in s for s in sqls)
    assert any("on conflict (layer) do update" in s for s in sqls)


def test_publish_if_green_moves_marker_only_on_green():
    from tests.conftest import FakeConn  # type: ignore

    conn = FakeConn()
    updated = publish_if_green(
        conn, layer="mart", dq_run_id="rid", must_pass=True, updated_by="t"
    )
    assert updated is True
    sqls = [s for s, _ in conn.executed]
    assert any("insert into dq.publication_marker" in s for s in sqls)

    conn2 = FakeConn()
    updated2 = publish_if_green(
        conn2, layer="mart", dq_run_id="rid2", must_pass=False
    )
    assert updated2 is False
    # no writes should have been issued
    assert conn2.executed == []
