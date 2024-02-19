from fastapi import APIRouter, Query
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from typing import List
from utils import parse_log_entry, generate_s3_key, upload_log_to_s3, round_to_nearest_hour, retrive_logs_from_s3_for_candidate
from collections import defaultdict
router = APIRouter()


@router.post('/ingest')
async def ingest_logs(logs:List[str]):
    try:
        log_counts = defaultdict(int)

        for log_entry in logs:
            timestamp, service, log_level, log_message = parse_log_entry(log_entry)
            rounded_timestamp = round_to_nearest_hour(timestamp)
            log_key = f"{rounded_timestamp}_{service}_{log_level}_{log_message}"
            log_counts[log_key] += 1
   
        for key, count in log_counts.items():
            rounded_timestamp, service, log_level, log_message = key.split('_')
            s3_key = generate_s3_key(rounded_timestamp, service, log_level)
            if upload_log_to_s3(log_message,count,s3_key):
                print(f"Log ingested successfully: {s3_key}")
            else:
                print(f"Log not ingested: {s3_key}")

        return JSONResponse(content={'message': 'Logs ingested successfully'}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
 
@router.get('/top-count')
async def top_count():
    try:
        logs  = retrive_logs_from_s3_for_candidate()
        return JSONResponse(content={'message': logs}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=e.status_code, detail=str(e.detail))
    
@router.get('/search-by-timestamp')
async def search_by_timestamp(start: str = Query(..., description="Start timestamp"),
                               end: str = Query(..., description="End timestamp")):
    try:
        # Filter data based on start and end timestamps
        logs = retrive_logs_from_s3_for_candidate(start_timestamp=start,end_timestamp=end,search_by_timestamp=True)
        
        # Return the filtered data as JSON
        return JSONResponse(content={'message': logs}, status_code=200)

    except Exception as e:
        # Handle exceptions if necessary
        return {"error": str(e)}


