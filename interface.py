from fastapi import FastAPI
import ETL

app = FastAPI()

@app.get("/")
def read_root():
    return "Logfile Assistant"

@app.post("/query")
def search(query: str):
    return ETL.search(query)