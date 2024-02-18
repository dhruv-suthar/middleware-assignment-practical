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

CANDIDATE_NAME = "test410"


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
    key = f"mw-code-tester/{CANDIDATE_NAME}/{timestamp}/{service}/{log_level}/summary.log"
    return key

def retrive_logs_from_s3(s3,key):
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        return content
    except Exception as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"The key '{key}' does not exist in the S3 bucket.")
        else:
            print(f"An error occurred: {e}")
        return ""              

def upload_logs_to_s3(log_message,log_count,key):
    try:
        existing_content = ""
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,region_name=AWS_REGION)
        existing_content = retrive_logs_from_s3(s3,key)
        updated_content = log_message
        # Append new content to existing content
        if existing_content != "":
                search_line = next((line for line in existing_content.split('\n') if log_message in line), None)
                existing_counter_value = log_count
                if search_line:
                    match = re.match(r"(\d+)", search_line)
                    if match:
                        # Extract existing counter value from the line
                        existing_counter_value = int(match.group(1))

                    # Increment the counter value by 1
                    updated_counter_value = existing_counter_value + 1
                    # Replace the existing counter value in the content with the updated value
                    updated_content = existing_content.replace(f"{existing_counter_value} - {log_message}", f"{updated_counter_value} - {log_message}")

                else:
                    log_message = f"{existing_counter_value} - {log_message}"
                    updated_content = existing_content + "\n" +  log_message 

        response = s3.put_object(Body=updated_content, Bucket=S3_BUCKET, Key=key)

        if response.get('ETag') != '':
             return True

        return False
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



def retrive_logs_from_s3_for_candidate():
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,region_name=AWS_REGION)

        # List all objects in the S3 bucket for the candidate
        objects = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f'mw-code-tester/{CANDIDATE_NAME}/')

        all_logs = []
        print(objects.get('Contents', []))
        for obj in objects.get('Contents', []):
                    key = obj['Key']

                    # Extract timestamp from the key
                    timestamp, service, level_type = key.split('/')[2:-1]

                    # Read logs from the object
                    logs  = retrive_logs_from_s3(s3,key).split('\n')
                    for log in logs:
                        all_logs.append({
                            'timestamp': timestamp,
                            'service': service,
                            'level_type': level_type,
                            'log': log
                        })
        
        return get_top_log(all_logs)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def get_top_log(all_logs):
     current_counter = 0
     count = 0
     top_error = ""
     service = ""
     severity = ""
     print(all_logs)
     for log in all_logs:
          match = re.match(r"(\d+)", log['log'])
          if match:
               if current_counter < int(match.group(1)):
                        current_counter = int(match.group(1))
                        count = current_counter
                        prefix = f"{str(match.group(1))} - "
                        top_error = log['log'][len(prefix):]
                        service = log['service']
                        severity = log['level_type']
    
     return {
          'top_error': top_error,
          'count': count,
          'service': service,
          'severity': severity
     }
                    

                
