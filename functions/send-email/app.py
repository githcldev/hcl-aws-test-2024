import os
import json
import boto3

def lambda_handler(event, context):
    try:
        print('Send email lambda_handler')
        jsonStr = json.dumps(event)
        print(jsonStr)

        if validate_input(event):
            print('input validation pass')
        else:
            print('input validation fail')
            raise AttributeError('input validation fail')

        send_notif(process_csv(event['Records'][0]['body']))

        return {
            'status': 200,
            'body': json.dumps('csv file processed')
        }
    except Exception as exc:
        print('caught error at send-email')
        print(type(exc).__name__)


def send_notif(email_msg):
    try:
        print('event_attribute_obj => ')
        sns_client = boto3.client('sns')
        result = sns_client.publish(
            TopicArn=os.environ.get('notif_arn'),
            Message=email_msg,
            Subject='Message hcl aws test from :: ranjan.sh@hcl.com'
        )
        print(json.dumps(result))
        print('success send notification')
    except Exception as err:
        print("An error occurred")
        raise err


def validate_input(event):
    try:
        print('validate input')
        print('event source -> input_params => ')
        if event['Records'][0] is not None:
            input_params = event['Records'][0]
            del input_params["receiptHandle"]
            del input_params["attributes"]
            print(json.dumps(input_params))
        else:
            print('Required attribute not found 1')
            raise AttributeError('Required attribute not found 1')

        queue_name = os.environ.get('queue_name')
        if input_params['eventSourceARN'].endswith(queue_name) and input_params['eventSource'] == 'aws:sqs':
            print('Correct sqs event source found 2')
        else:
            print('Event source is not found as required 2')
            raise AttributeError('Event source is not as required')

        # read_csv_file_name = event['Records'][0]["messageAttributes"]
        event_attribute = input_params['body']
        print('read => ', event_attribute)
        return True
    except:
        print('input validation failed')
        return False


def process_csv(event_attribute):
    try:
        event_attribute_obj = json.loads(event_attribute)
        print('event_attribute_obj => ')
        print(json.dumps(event_attribute_obj))

        if event_attribute_obj["csv_file_name"] is None:
            print("required attribute for csv read not found 3")
            raise AttributeError("required attribute for csv read not found")

        s3_resource = boto3.client('s3')
        result = s3_resource.get_object(
            Bucket = os.environ.get('bucket_name'),
            Key = event_attribute_obj["csv_file_name"]
        )
        csv_content = result['Body'].read().decode('utf-8')
        print(json.dumps(csv_content))
        email_msg = csv_content.replace('\n', ' => ')
        return email_msg
    except Exception as err:
        print("An error occurred")
        raise err

