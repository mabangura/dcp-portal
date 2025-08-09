from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd, io, base64, json
app = FastAPI(title="DCP Cleaner", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
class Z(BaseModel): columns:list[str]; threshold:float=3.0
class B(BaseModel): column:str; min:float|None=None; max:float|None=None
class R(BaseModel): drop_duplicates:list[str]|None=None; fillna:dict[str,str|float|int]|None=None; zscore_outliers:Z|None=None; bounds_check:list[B]|None=None
def to_df(b:bytes):
    try: return pd.read_csv(io.BytesIO(b))
    except Exception: return pd.read_json(io.BytesIO(b))
@app.get("/health")
def h(): return {"ok":True}
@app.post("/clean/run")
async def run(file:UploadFile=File(...), rules:str=Form("{}"), preview_only:str=Form("true")):
    df=to_df(await file.read())
    try: r=R(**json.loads(rules or "{}"))
    except Exception as e: return JSONResponse({"error":f"Invalid rules JSON: {e}"},400)
    before=len(df); rep={"before_rows":int(before),"actions":[]}
    if r.drop_duplicates: du=df.duplicated(subset=r.drop_duplicates).sum(); df=df.drop_duplicates(subset=r.drop_duplicates); rep["actions"].append({"drop_duplicates":int(du)})
    if r.fillna:
        for c,v in r.fillna.items():
            if c in df.columns: n=df[c].isna().sum(); df[c]=df[c].fillna(v); rep["actions"].append({"fillna":{c:int(n)}})
    if r.zscore_outliers and r.zscore_outliers.columns:
        fl={}
        for c in [c for c in r.zscore_outliers.columns if c in df.columns]:
            s=pd.to_numeric(df[c],errors="coerce"); z=(s-s.mean())/(s.std(ddof=0) if s.std(ddof=0)!=0 else 1); m=z.abs()>r.zscore_outliers.threshold
            fl[c]=int(m.sum()); if preview_only.lower()!="true": df=df[~m]
        rep["actions"].append({"zscore_outliers":fl})
    if r.bounds_check:
        tot=0
        for rule in r.bounds_check:
            if rule.column in df.columns:
                s=pd.to_numeric(df[rule.column],errors="coerce"); ok=(s>=(rule.min if rule.min is not None else s.min())) & (s<=(rule.max if rule.max is not None else s.max()))
                tot+=int((~ok).sum()); if preview_only.lower()!="true": df=df[ok]
        rep["actions"].append({"bounds_check_flagged":tot})
    after=len(df); rep["after_rows"]=int(after); rep["rows_changed"]=int(before-after)
    out=base64.b64encode(df.to_csv(index=False).encode()).decode()
    return {"report":rep,"cleaned_csv_base64":out}
