from fastapi import Request
from uuid import uuid4

async def demo_middleware(request:Request, call_next):
    random_uuid = str(uuid4())

    print("radom_uuid >> ",random_uuid)
    request.state.random_uuid = random_uuid

    response = await call_next(request)

    return response






