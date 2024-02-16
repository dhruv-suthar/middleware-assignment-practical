## Installation

1. Install FastAPI project requirements:

   ```bash
   pip install -r requirements.txt

   ```

2. Update AWS configuration in .env file.

3. Navigate to the `log_backend_server` folder and run

```bash
 uvicorn main:app --host 0.0.0.0 --port 8002 --reload
```

## Endpoints

2. Ingest - POST `/ingest`

#### Request Example

```json
[
  "2023-10-11T10:31:00Z INFO [apache] Received GET request from 192.168.0.1 for /index.html",
  "2023-10-11T10:32:15Z INFO [apache] Request from 10.0.0.2 failed with status code 404 for /page-not-found.html",
  "2023-10-11T11:33:30Z WARN [nginx] Received POST request from 192.168.0.3 for /submit-form",
  "2023-10-11T11:34:45Z WARN [nginx] Timeout warning for request from 192.168.0.4 to /api/data",
  "2023-10-11T11:35:45Z WARN [nginx]  Timeout warning for request from 192.168.0.4 to /api/data"
]
```

#### Response

```json
{
  "message": "Logs ingested successfully"
}
```

2. Top error with count - GET `/top-count`

#### Response

```
{
    “top-error”: “Timeout warning for request from 192.168.0.4 to /api/data”,
    ‘count“: 2,
    “service” : “nginx”
    “severity”: “WARN”
}

```
