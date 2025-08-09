from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd
import io, base64, json

app = FastAPI(title="DataCollect Pro Cleaner", version="0.1.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

class ZScoreOutliers(BaseModel):
    columns: list[str]
    threshold: float = 3.0

class BoundsRule(BaseModel):
    column: str
    min: float | None = None
    max: float | None = None

class Ruleset(BaseModel):
    drop_duplicates: list[str] | None = None
    fillna: dict[str, str | float | int] | None = None
    zscore_outliers: ZScoreOutliers | None = None
    bounds_check: list[BoundsRule] | None = None

def to_df(b: bytes) -> pd.DataFrame:
    try:
        return pd.read_csv(io.BytesIO(b))
    except Exception:
        return pd.read_json(io.BytesIO(b))

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/rulesets")
def rulesets_example():
    return {
        "example": {
            "drop_duplicates": ["submission_id"],
            "fillna": {"village": "UNKNOWN"},
            "zscore_outliers": {"columns": ["age", "plot_size_ha"], "threshold": 3.0},
            "bounds_check": [
                {"column": "gps_lat", "min": -90, "max": 90},
                {"column": "gps_lon", "min": -180, "max": 180},
            ],
        }
    }

@app.post("/clean/run")
async def clean_run(
    file: UploadFile = File(...),
    rules: str = Form("{}"),
    preview_only: str = Form("true"),
):
    content = await file.read()
    df = to_df(content)

    # Parse rules
    try:
        ruleset = Ruleset(**json.loads(rules or "{}"))
    except Exception as e:
        return JSONResponse({"error": f"Invalid rules JSON: {e}"}, status_code=400)

    before_rows = len(df)
    report: dict = {"before_rows": int(before_rows), "actions": []}

    # 1) Drop duplicates
    if ruleset.drop_duplicates:
        dupe_count = int(df.duplicated(subset=ruleset.drop_duplicates).sum())
        df = df.drop_duplicates(subset=ruleset.drop_duplicates)
        report["actions"].append({"drop_duplicates": dupe_count})

    # 2) Fill NA
    if ruleset.fillna:
        for col, val in ruleset.fillna.items():
            if col in df.columns:
                n = int(pd.isna(df[col]).sum())
                df[col] = df[col].fillna(val)
                report["actions"].append({"fillna": {col: n}})

    # 3) Z-score outliers
    if ruleset.zscore_outliers and ruleset.zscore_outliers.columns:
        flagged: dict[str, int] = {}
        for col in [c for c in ruleset.zscore_outliers.columns if c in df.columns]:
            s = pd.to_numeric(df[col], errors="coerce")
            std = s.std(ddof=0)
            z = (s - s.mean()) / std if std != 0 else pd.Series([0] * len(s), index=s.index)
            mask = z.abs() > ruleset.zscore_outliers.threshold
            flagged[col] = int(mask.sum())
            if preview_only.lower() != "true":
                df = df[~mask]
        report["actions"].append({"zscore_outliers": flagged})

    # 4) Bounds check
    if ruleset.bounds_check:
        total_flagged = 0
        for rule in ruleset.bounds_check:
            if rule.column in df.columns:
                s = pd.to_numeric(df[rule.column], errors="coerce")
                lower_ok = s >= (rule.min if rule.min is not None else s.min())
                upper_ok = s <= (rule.max if rule.max is not None else s.max())
                ok_mask = lower_ok & upper_ok
                total_flagged += int((~ok_mask).sum())
                if preview_only.lower() != "true":
                    df = df[ok_mask]
        report["actions"].append({"bounds_check_flagged": int(total_flagged)})

    after_rows = len(df)
    report["after_rows"] = int(after_rows)
    report["rows_changed"] = int(before_rows - after_rows)

    cleaned_csv = df.to_csv(index=False).encode("utf-8")
    out_b64 = base64.b64encode(cleaned_csv).decode("utf-8")

    return {"report": report, "cleaned_csv_base64": out_b64}
