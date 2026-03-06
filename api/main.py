from fastapi import FastAPI, Depends
from security import verify_token
import routers

app = FastAPI(
    title="Compare Trips API - Train vs Plane",
    description="API permettant de comparer l'empreinte carbone entre l'avion et le train.",
    version="1.0.0",
    dependencies=[Depends(verify_token)]
)

app.include_router(routers.router, prefix="/api/v1")

@app.get("/", summary="Root endpoint")
def read_root():
    return {"message": "Bienvenue sur l'API Compare Trips. Accédez à /docs pour explorer les endpoints."}
