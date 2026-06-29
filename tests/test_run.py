"""Unit tests for the run entrypoint's step selection (no real pipeline I/O).

The pipeline functions are monkeypatched to record-only stubs, so we test that `--no-serve`
runs generate -> silver -> dbt and skips serve, while the default runs all four.
"""
from vitals import run


def _patch(monkeypatch, called):
    monkeypatch.setattr("vitals.generate.generate", lambda: called.append("generate"))
    monkeypatch.setattr("vitals.lakehouse.build", lambda: called.append("silver"))
    monkeypatch.setattr("vitals.run._dbt_build", lambda: called.append("dbt"))
    monkeypatch.setattr("vitals.serve.run", lambda: called.append("serve"))


def test_no_serve_runs_data_steps_only(monkeypatch):
    called = []
    _patch(monkeypatch, called)
    run.main(["--no-serve"])
    assert called == ["generate", "silver", "dbt"]


def test_default_runs_all_four_steps(monkeypatch):
    called = []
    _patch(monkeypatch, called)
    run.main([])
    assert called == ["generate", "silver", "dbt", "serve"]
