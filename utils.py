import boto3
from fastapi import HTTPException
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import re

load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = 'mw-code-tester'
AWS_REGION =  'ap-south-1'

CANDIDATE_NAME = "DHRUV-SUTHAR"

def handle_s3_exception(e):
    error_code = e.response['Error']['Code']
    if error_code == 'NoSuchKey':
        print(f"The key does not exist in the S3 bucket.")
        return ""
    elif error_code == 'NoSuchBucket':
        raise HTTPException(status_code=404, detail=str(e))
    elif error_code == 'AccessDenied':
        raise HTTPException(status_code=403, detail=str(e))
    elif error_code in ['InvalidAccessKeyId', 'SignatureDoesNotMatch']:
        raise HTTPException(status_code=400, detail=str(e))
    else:
        raise HTTPException(status_code=500, detail=str(e))

def round_to_nearest_hour(timestamp_str):
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
    # Round the timestamp to the nearest hour
    rounded_timestamp = (timestamp.replace(second=0, microsecond=0, minute=0))
    # Add one more hour to the rounded timestamp
    rounded_and_added_hour_timestamp = rounded_timestamp + timedelta(hours=1)
    formatted_timestamp = rounded_and_added_hour_timestamp.strftime("%H:%M:%S")
    return f"{rounded_timestamp.isoformat()}-{formatted_timestamp}"

def parse_log_entry(log_entry):
    timestamp_str, log_level, service, log_message = log_entry.split(' ', 3)
    service = service.strip('[]')
    return timestamp_str, service, log_level, log_message.strip()

def generate_s3_key(timestamp, service, log_level):
    key = f"{CANDIDATE_NAME}/{timestamp}/{service}/{log_level}/summary.log"
    return key

def retrive_logs_from_s3(s3,key):
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        return content
    except Exception as e:
        return handle_s3_exception(e)

             
def upload_log_to_s3(log_message,log_count,key):
    try:
        existing_content = ""
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,region_name=AWS_REGION)
        existing_content = retrive_logs_from_s3(s3,key)
        updated_content = ""
        # Append new content to existing content
        if existing_content != "":
                search_line = next((line for line in existing_content.split('\n') if log_message in line), None)
                existing_counter_value = log_count
                if search_line:
                    # extracting digits at the beginning for particular line
                    match = re.match(r"(\d+)", search_line)
                    if match:
                        existing_counter_value = int(match.group(1))

                    # Increment the counter value by 1
                    updated_counter_value = existing_counter_value + 1
                    # Replace the existing counter value in the content with the updated value
                    updated_content = existing_content.replace(f"{existing_counter_value} - {log_message}", f"{updated_counter_value} - {log_message}")

                else:
                    log_message = f"{existing_counter_value} - {log_message}"
                    updated_content = existing_content + "\n" +  log_message 
        else:
            # add message to new file   
            log_message = f"{log_count} - {log_message}"
            updated_content = log_message

        response = s3.put_object(Body=updated_content, Bucket=S3_BUCKET, Key=key)

        if response.get('ETag') != '':
             return True
        return False
    except Exception as e:
        return handle_s3_exception(e)

def retrive_logs_from_s3_for_candidate():
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,region_name=AWS_REGION)
        # List all objects in the S3 bucket for the candidate
        objects = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f'{CANDIDATE_NAME}/')

        all_logs = []
        for obj in objects.get('Contents', []):
                    key = obj['Key']
                    # Extract timestamp from the key
                    timestamp, service, level_type = key.split('/')[1:-1]
                    # Read logs from the object
                    logs  = retrive_logs_from_s3(s3,key).split('\n')
                    for log in logs:
                        all_logs.append({
                            'timestamp': timestamp,
                            'service': service,
                            'severity': level_type,
                            'log': log
                        })
        
        return get_top_log(all_logs) if all_logs else f'No logs found for this candidate: {CANDIDATE_NAME}'
    except Exception as e:
        return handle_s3_exception(e)
         
def get_top_log(all_logs):
    # Find the log with the highest count by extracting digits at the beginning of particular log  
    max_log = max(all_logs, key=lambda log: int(re.match(r"(\d+)", log['log']).group(1)), default=None)

    # Extract the top error, count, service, and severity type from the max log
    if max_log:
        match = re.match(r"(\d+)", max_log['log'])
        count = int(match.group(1))
        prefix = f"{str(count)} - "
        top_error = max_log['log'][len(prefix):]
        service = max_log['service']
        severity = max_log['severity']

        return {
            'top_error': top_error,
            'count': count,
            'service': service,
            'severity': severity
        }
    else:
         return {}
                    