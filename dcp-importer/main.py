from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd, io, base64, json
app=FastAPI(title="DCP Importer",version="0.1.0")
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True,allow_methods=["*"],allow_headers=["*"])
class Dry(BaseModel): rows:int; cols:int; columns:list[str]; missing_required:list[str]; sample:list[dict]|None=None
def to_df(b:bytes):
    try:return pd.read_csv(io.BytesIO(b))
    except Exception:return pd.read_json(io.BytesIO(b))
@app.get("/health")
def h(): return {"ok":True}
@app.post("/import/dryrun")
async def dry(file:UploadFile=File(...), required_columns:str=Form("[]")):
    df=to_df(await file.read())
    try:req=json.loads(required_columns or "[]"); req=req if isinstance(req,list) else []
    except Exception:req=[]
    miss=[c for c in req if c not in df.columns]; sample=df.head(5).to_dict(orient="records")
    return Dry(rows=int(len(df)), cols=int(len(df.columns)), columns=list(df.columns), missing_required=miss, sample=sample)
@app.post("/import/commit")
async def commit(file:UploadFile=File(...), form_id:str=Form(...), mapping:str=Form("{}")):
    _=await file.read()
    try:m=json.loads(mapping or "{}")
    except Exception:m={}
    return {"jobId":"job_mock_001","formId":form_id,"mapping":m,"status":"queued"}
@app.post("/merge/run")
async def merge(left:UploadFile=File(...), right:UploadFile=File(...), left_key:str=Form(...), right_key:str=Form(...), how:str=Form("left")):
    ldf=to_df(await left.read()); rdf=to_df(await right.read())
    if left_key not in ldf.columns or right_key not in rdf.columns: return JSONResponse({"error":"Join keys not found"},400)
    merged=ldf.merge(rdf, left_on=left_key, right_on=right_key, how=how)
    out=base64.b64encode(merged.to_csv(index=False).encode()).decode()
    lin={"left_columns":list(ldf.columns),"right_columns":list(rdf.columns),"merged_columns":list(merged.columns),"how":how,"left_key":left_key,"right_key":right_key,"rows":int(len(merged))}
    return {"report":lin,"merged_csv_base64":out}
