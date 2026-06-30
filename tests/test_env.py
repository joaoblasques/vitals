from pathlib import Path

from vitals import env


def test_bronze_dir_defaults_to_repo_data_bronze(monkeypatch):
    monkeypatch.delenv("VITALS_BRONZE_DIR", raising=False)
    assert env.bronze_dir() == Path(env.__file__).resolve().parents[2] / "data" / "bronze"


def test_bronze_dir_honors_override(monkeypatch, tmp_path):
    monkeypatch.setenv("VITALS_BRONZE_DIR", str(tmp_path))
    assert env.bronze_dir() == tmp_path


def test_spark_mode_defaults_to_serverless(monkeypatch):
    monkeypatch.delenv("VITALS_SPARK_MODE", raising=False)
    assert env.spark_mode() == "serverless"


def test_spark_mode_ambient_when_set(monkeypatch):
    monkeypatch.setenv("VITALS_SPARK_MODE", "ambient")
    assert env.spark_mode() == "ambient"


def test_generate_writes_to_override_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("VITALS_BRONZE_DIR", str(tmp_path))
    from vitals import generate
    generate.generate()
    assert (tmp_path / "patients.ndjson").exists()
    assert (tmp_path / "patients.ndjson").stat().st_size > 0
