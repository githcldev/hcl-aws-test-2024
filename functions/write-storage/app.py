from datetime import datetime
import os
import json
import base64
import boto3

batch_name_str = ''
csv_file_name_str = ''

def lambda_handler(event, context):
    try:
        print('Write storage lambda_handler')
        # jsonStr = json.dumps(event); print(jsonStr)
        print('env variable => ')
        if 'queue_url' in os.environ: print(os.environ.get('queue_url'))
        else:
            print('Queue url not found')
            raise ValueError('Queue url not found')
        if os.environ.get('bucket_name') is not None: print(os.environ.get('bucket_name'))
        else:
            print('bucket name to store file not found')
            raise ValueError('bucket name to store file not found')
        
        batch_name()
        if write_file_s3(event["body"]):
            send_email_event()
        else:
            raise RuntimeError('Error during file write')
    except Exception as exc:
        print('exc 1')
        # print(exc.message)
        print(type(exc).__name__)
        return {
            'status': 200,
            'body': json.dumps('Error caught!')
        }
    finally:
        return {
            'status': 200,
            'body': json.dumps('Queue event triggered for sending mail!')
        }
        


def send_email_event():
    try:
        global batch_name_str
        global csv_file_name_str
        sqs = boto3.resource('sqs', region_name='eu-north-1')
        queue_url = os.environ.get('queue_url')
        queue_name = os.environ.get('queue_name')
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        msg_dict = {
            "batch_name": batch_name_str,
            "csv_file_name": csv_file_name_str
        }
        msg_body = json.dumps(msg_dict)
        print('sent message => ', msg_body)
        result = queue.send_message(
            QueueUrl=queue_url,
            MessageBody=msg_body
        )
        print('message sent success => ', json.dumps(result))
        return True
    except Exception as exc:
        print('error send_email_event => ')
        print(exc.message)
        print(type(exc).__name__)


def write_file_s3(body_str) -> bool:
    try:
        global batch_name_str
        global csv_file_name_str
        if len(body_str) < 1:
            print('body string not found')
            raise ValueError('body string not found')
        csv_dt = base64.b64decode(body_str).decode('utf-8')
        print('csv_dt => ', csv_dt)
        parts = csv_dt.split("\r\n")
        writeParts = [parts[4], parts[5]]
        finalPrint = '\n'.join(writeParts)
        print('writeParts => ', writeParts)
        print('finalPrint => ', finalPrint)
        # print('batch_name => ', batch_name_str)
        csv_file_name_str = '{}.csv'.format(batch_name_str)
        print('csv_fn => ', csv_file_name_str)

        s3_resource = boto3.resource('s3')
        s3_resource.Object(os.environ.get('bucket_name'), csv_file_name_str).put(Body=finalPrint)
        # s3_resource.Object(bucket, 'df.csv').put(Body=finalPrint)
        return True
    except:
        print('error while s3 file write')
        return False


def batch_name() -> str:
    try:
        global batch_name_str
        print('batch name calc')
        dt = datetime.now()
        batch_name_str = "{0}-{1}-{2}_{3}-{4}-{5}".format(dt.year, dt.month,
            dt.day, dt.hour, dt.minute, dt.second)
        return batch_name_str
    except:
        print('error in batch name calc')

