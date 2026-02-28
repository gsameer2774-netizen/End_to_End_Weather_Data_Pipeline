import json
import boto3
import csv
import io

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    try:
        # 1. Fetch the CSV file
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        # 2. Read CSV data
        reader = csv.DictReader(io.StringIO(content))
        data_list = list(reader)
        
        if not data_list:
            return {'statusCode': 200, 'body': "File was empty"}

        # 3. Handle Both Column Names
        for row in data_list:
            # Check for 'temp_cels' or 'temp_celsius'
            temp_value = row.get('temp_cels') or row.get('temp_celsius')
            
            if temp_value:
                # Standardize it to one name for your Monday Glue Crawler
                row['standard_temp_celsius'] = float(temp_value)
        
        # 4. Save to processed folder
        processed_key = f"processed/{key.replace('.csv', '_processed.json')}"
        
        s3_client.put_object(
            Bucket=bucket,
            Key=processed_key,
            Body=json.dumps(data_list)
        )
        
        return {
            'statusCode': 200, 
            'body': f"Success! Handled data and saved to {processed_key}"
        }
        
    except Exception as e:
        print(f"Error: {e}")
        raise e
