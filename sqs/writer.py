import boto3
import numpy as np
from util import constant as con

"""
        
        ****    Template for writer     ****
                -------------------
    
    
        sqs = boto3.client('sqs', region_name="us-west-2",
                    aws_access_key_id='',
                    aws_secret_access_key=''
                )
        queue_url = ''
        
        
        response = sqs.send_message(
            QueueUrl=queue_url,
            DelaySeconds=10,
            MessageAttributes={
                'Title': {
                    'DataType': 'String',
                    'StringValue': 'The Whistler'
                },
                'Author': {
                    'DataType': 'String',
                    'StringValue': 'John Grisham'
                },
                'WeeksOn': {
                    'DataType': 'Number',
                    'StringValue': '6'
                }
            },
            MessageBody=(
                'Information about current NY Times fiction bestseller for '
                'week of 12/11/2016.'
            )
        )
        
        print(response['MessageId'])
"""

def send_data_to_sqs(record: np.array = np.array([]), groupId: str = None):
    print('send_data_to_sqs')
    if record is None:
        print('No record found')
        return None

    sqs = boto3.client('sqs',
                       aws_access_key_id= con.ACCESS_KEY,
                       aws_secret_access_key= con.SECRET_CODE
                      )
    queue_arn = 'https://sqs.us-west-2.amazonaws.com/131412264981/oa_data'
    response = sqs.send_message(
        QueueUrl=queue_arn,
        DelaySeconds=10,
        MessageBody=np.array_str(record)
    )
    print(response['MessageId'])