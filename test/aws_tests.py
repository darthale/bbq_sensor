import boto3
import json
from boto3 import resource
from boto3.dynamodb.conditions import Key

S3_BUCKET = "cleverbbq"
DATA = "temperature_data"
FILE_EXT = "json"
THRESHOLD = 30
LAST_ALERT = "last_alert.json"
s3_client = boto3.client("s3")
dynamodb_client = resource('dynamodb')

resp = s3_client.list_objects_v2(Bucket="cleverbbq", Prefix="temperature_data/20180419")

temp_obj = resp["Contents"]

sorted(temp_obj, key=lambda item: item["LastModified"])

for objs in temp_obj:
    print(objs["Key"])

s3_session = "20180419"

"""sns_client = boto3.client("sns")

sns_client.publish(
    TopicArn="arn:aws:sns:eu-west-2:361964164915:temperature-threshold",
    Message="test", Subject="test subj")"""

"""# checking if we already have an s3 session
    is_session = False
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=DATA)
    for obj in response["Contents"]:
        if obj["Key"] == DATA + "/" + s3_session:
            logger.info("Session already exists")
            is_session = True
            break"""

"""s3_file = \
s3_client.get_object(Bucket=S3_BUCKET, Key=s3_session + "slopes.json")[
    "Body"].read()"""

"""table = dynamodb_client.Table("alert_history")
response = table.get_item(Key={"session": "20180506"})
print(table.item_count)
print(response["Item"])
table.update_item(Key={"session": "20180506"},
                  UpdateExpression="SET alert = :val",
                  ExpressionAttributeValues={":val": "test"})

table.put_item(Item={"session": "20180505", "alert": "teeeeeeeee"})"""
