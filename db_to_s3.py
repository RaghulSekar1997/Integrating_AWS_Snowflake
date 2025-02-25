# put records from DynomoDB to S3

import json
import csv
import boto3
import os
import time
from io import StringIO
from datetime import datetime

s3 = boto3.client('s3')

# S3 bucket and folder path
BUCKET_NAME = "project-weather-raghul"
FOLDER_NAME = "snowflake/"

def lambda_handler(event, context):
    records = []
    print(event)

    # Process each record from DynamoDB stream
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            new_image = record['dynamodb']['NewImage']
            record_data = {
                'city': new_image['city']['S'],
                'time': new_image['time']['S'],
                'temp': new_image['temp']['N'],
                'wind_speed': new_image['wind_speed']['N'],
                'wind_dir': new_image['wind_dir']['S'],
                'pressure_mb': new_image['pressure_mb']['N'],
                'humidity': new_image['humidity']['N']
            }
            records.append(record_data)

    # If no records to process, exit
    if not records:
        return {
            'statusCode': 200,
            'body': json.dumps('No new records to process')
        }

    # Convert records to CSV format
    csv_buffer = StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=records[0].keys())
    csv_writer.writeheader()
    csv_writer.writerows(records)

    # Generate a unique file name using timestamp
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"{FOLDER_NAME}weather_data_{timestamp}.csv"

    # Upload to S3
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )

    return {
        'statusCode': 200,
        'body': json.dumps(f"CSV file {file_name} uploaded successfully to S3")
    }
