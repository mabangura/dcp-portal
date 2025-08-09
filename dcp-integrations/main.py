from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import base64, time
app=FastAPI(title="DCP Integrations",version="0.1.0")
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True,allow_methods=["*"],allow_headers=["*"])
class Hook(BaseModel): url:str|None=None; secret:str|None=None; event:str
class Sheet(BaseModel): sheet_name:str; csv_base64:str
class Rapid(BaseModel): flow_uuid:str; contact:str; fields:dict[str,str]|None=None
@app.get("/health")
def h(): return {"ok":True}
@app.post("/hooks/test")
def hook(h:Hook): return {"ok":True,"event":h.event,"signature_preview":((h.secret or "")[:4]+"***") if h.secret else None}
@app.post("/sheets/sync")
def sheet(s:Sheet):
    b=base64.b64decode(s.csv_base64.encode()); p=f"/tmp/{int(time.time())}_{s.sheet_name.replace(' ','_')}.csv"
    open(p,"wb").write(b); return {"ok":True,"written":p}
@app.post("/rapidpro/send")
def rp(r:Rapid): return {"ok":True,"flow_uuid":r.flow_uuid,"contact":r.contact,"fields":r.fields or {}}
