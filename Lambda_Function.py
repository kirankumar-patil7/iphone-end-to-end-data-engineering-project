import json
import pandas as pd
import boto3
from io import StringIO 
import io
from datetime import datetime

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'iphone-analysis-kirankumar'
    Key = 'source_data/'
    # print(s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents'])
    for file in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        if file['Key'].split('.')[-1] == 'csv':
            print(file['Key'])
            response = s3.get_object(Bucket = Bucket, Key = file['Key'])
            content = response['Body'].read().decode('utf-8')
            print(content)
    df = pd.read_csv(io.StringIO(content)) 
    # print(df.info())

    df1=df[df['Star Rating'] > 4.5]

    apple_key = "target_transformed_data/apple_transformed_" + str(datetime.now()) + ".csv"
    apple_buffer=StringIO()
    df1.to_csv(apple_buffer, index=False)
    apple_content = apple_buffer.getvalue()
    s3.put_object(Bucket=Bucket, Key=apple_key, Body=apple_content)
    




    

            