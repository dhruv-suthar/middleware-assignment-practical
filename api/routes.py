from fastapi import APIRouter
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from typing import List
from utils import parse_log_entry, generate_s3_key, upload_logs_to_s3, round_to_nearest_hour, retrive_logs_from_s3_for_candidate

router = APIRouter()


@router.post('/ingest')
async def ingest_logs(logs:List[str]):
    try:
        parsed_logs = []
        for log_entry in logs:
            timestamp, service, log_level, log_message = parse_log_entry(log_entry)
            rounded_timestamp = round_to_nearest_hour(timestamp)
            parsed_logs.append(log_message)
            s3_key = generate_s3_key(rounded_timestamp, service, log_level)
            if upload_logs_to_s3(log_message, s3_key):
                print(f"Log ingested successfully: {s3_key}")
            else:
                print(f"Log not ingested: {s3_key}")
        return JSONResponse(content={'message': 'Logs ingested successfully'}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
 


@router.get('/top-count')
async def top_count():
    try:
        logs  = retrive_logs_from_s3_for_candidate()
        return JSONResponse(content={'message': logs}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    