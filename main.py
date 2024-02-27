from fastapi import FastAPI
from api.routes import router
from middleware import demo_middleware

app = FastAPI()

app.include_router(router)
app.middleware("http")(demo_middleware)

