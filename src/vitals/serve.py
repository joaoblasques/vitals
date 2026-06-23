"""AI-ready serving layer — the three things a health-tech team does with clean data.

1. Feature store  : per-patient, time-aware features (offline table + parquet; Feast repo in ml/).
2. Vector index   : TF-IDF embeddings of clinical notes + a cosine RAG query (pgvector in prod).
3. Demo model     : surgery-risk classifier consuming the features, tracked in MLflow.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DB = ROOT / "data" / "vitals.duckdb"
GOLD = ROOT / "data" / "gold"
RESULTS = ROOT / "data" / "results.json"

FEATURE_SQL = """
with agg as (
    select patient_key,
        count(*)                                                      as n_observations,
        avg(value_std) filter (where metric='pain')                  as mean_pain,
        arg_max(value_std, obs_date) filter (where metric='pain')    as last_pain,
        arg_max(value_std, obs_date) filter (where metric='pain')
          - arg_min(value_std, obs_date) filter (where metric='pain') as pain_trend,
        avg(value_std) filter (where metric='adherence')             as mean_adherence,
        avg(value_std) filter (where metric='glucose')               as mean_glucose_mgdl,
        avg(value_std) filter (where metric='heart_rate')            as mean_hr
    from gold.fct_observation group by 1
)
select d.patient_key, d.age,
       case when d.gender='male' then 1 when d.gender='female' then 0 else null end as gender_male,
       d.primary_condition_code,
       a.n_observations, a.mean_pain, a.last_pain, a.pain_trend,
       a.mean_adherence, a.mean_glucose_mgdl, a.mean_hr,
       d.surgery_90d
from gold.dim_patient d join agg a using (patient_key)
"""


def run() -> dict:
    GOLD.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB))

    # ---------- 1. FEATURE STORE (offline table + parquet) ----------
    feats = con.execute(FEATURE_SQL).df()
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute("CREATE OR REPLACE TABLE gold.patient_features AS SELECT * FROM feats")
    feats.to_parquet(GOLD / "patient_features.parquet", index=False)

    # ---------- 2. VECTOR INDEX + RAG demo ----------
    notes = con.execute("SELECT patient_key, text FROM silver.note").df()
    rag = _rag_demo(notes, queries=[
        "severe lower back pain worse with sitting, poor adherence",
        "shoulder pain with overhead reaching",
    ])

    # ---------- 3. DEMO MODEL (surgery risk) ----------
    model_metrics = _train_model(feats)

    con.close()

    results = {
        "data_quality": json.loads((ROOT / "data" / "dq_report.json").read_text()),
        "feature_store": {
            "n_patients": int(len(feats)),
            "features": [c for c in feats.columns if c not in ("patient_key", "surgery_90d")],
            "offline_table": "gold.patient_features",
            "parquet": "data/gold/patient_features.parquet",
        },
        "vector_index": rag,
        "model": model_metrics,
    }
    RESULTS.write_text(json.dumps(results, indent=2, default=str))
    print(json.dumps(results, indent=2, default=str))
    return results


def _rag_demo(notes: pd.DataFrame, queries: list[str]) -> dict:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    notes = notes.drop_duplicates(subset="text").reset_index(drop=True)
    vec = TfidfVectorizer(stop_words="english", min_df=2)
    mat = vec.fit_transform(notes["text"])
    out = []
    for q in queries:
        sims = cosine_similarity(vec.transform([q]), mat).ravel()
        top = sims.argsort()[::-1][:3]
        out.append({
            "query": q,
            "matches": [{"score": round(float(sims[i]), 3), "note": notes.iloc[i]["text"][:160]} for i in top],
        })
    return {
        "n_notes_indexed": int(len(notes)),
        "embedding": "TF-IDF (prod target: pgvector + clinical embeddings)",
        "vocab_size": int(len(vec.vocabulary_)),
        "demo_queries": out,
    }


def _train_model(feats: pd.DataFrame) -> dict:
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score, accuracy_score
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import StandardScaler

    feat_cols = ["age", "gender_male", "n_observations", "mean_pain", "last_pain",
                 "pain_trend", "mean_adherence", "mean_glucose_mgdl", "mean_hr"]
    X = feats[feat_cols]
    y = feats["surgery_90d"].astype(int)
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

    pipe = Pipeline([
        ("impute", SimpleImputer(strategy="median")),
        ("scale", StandardScaler()),
        ("clf", LogisticRegression(max_iter=1000)),
    ])
    pipe.fit(Xtr, ytr)
    proba = pipe.predict_proba(Xte)[:, 1]
    auc = roc_auc_score(yte, proba)
    acc = accuracy_score(yte, pipe.predict(Xte))
    coefs = dict(zip(feat_cols, np.round(pipe.named_steps["clf"].coef_.ravel(), 3).tolist()))

    # MLflow tracking (local file store)
    try:
        import mlflow
        mlflow.set_tracking_uri(f"sqlite:///{ROOT/'mlflow.db'}")
        mlflow.set_experiment("vitals-surgery-risk")
        with mlflow.start_run(run_name="logreg-mvp"):
            mlflow.log_params({"model": "logreg", "n_features": len(feat_cols), "n_train": len(Xtr)})
            mlflow.log_metrics({"roc_auc": float(auc), "accuracy": float(acc)})
        tracked = True
    except Exception as e:  # pragma: no cover - mlflow is best-effort
        tracked = f"skipped: {e}"

    return {
        "task": "surgery_within_90d (binary)",
        "model": "LogisticRegression",
        "n_train": int(len(Xtr)), "n_test": int(len(Xte)),
        "roc_auc": round(float(auc), 3), "accuracy": round(float(acc), 3),
        "positive_rate": round(float(y.mean()), 3),
        "top_coefficients": coefs,
        "mlflow_tracked": tracked,
    }


if __name__ == "__main__":
    run()
