from vitals import lakehouse, phi


def test_build_checks_phi_on_every_silver_table(monkeypatch, tmp_path):
    """build() must call phi.assert_no_phi for all 8 silver tables, not just patient."""
    checked_tables = []
    original = phi.assert_no_phi

    def spy(table, columns):
        checked_tables.append(table)
        return original(table, columns)

    monkeypatch.setattr(lakehouse.phi, "assert_no_phi", spy)
    monkeypatch.setattr(lakehouse, "DB", tmp_path / "vitals.duckdb")
    monkeypatch.setattr(lakehouse, "DQ_OUT", tmp_path / "dq_report.json")
    monkeypatch.setattr(lakehouse, "SILVER_BASELINE", tmp_path / "silver_baseline.json")

    lakehouse.build()

    assert set(checked_tables) == {
        "patient", "target", "observation", "condition",
        "note", "claim", "pro", "wearable_daily",
    }
