from fastapi import FastAPI, Request
import requests

from lambda_function import lambda_handler

app = FastAPI()


@app.post("/")
async def root(
        request: Request
):
    print('================================PAYLOAD================================')
    event = await request.json()
    lambda_handler(event.get('event'), None)
    return {"message": "Hello World"}


@app.get("/")
async def root(
):
    requests.get('https://www.google.com')
    print('================================PAYLOAD================================')
    return {"message": "Hello World"}
