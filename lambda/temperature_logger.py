import json
import logging
import boto3
from datetime import datetime
import time

s3_client = boto3.client("s3")

S3_BUCKET = "cleverbbq"
DATA = "temperature_data"
FILE_EXT = "json"
THRESHOLD = 30  # celsius
INTERVAL_BETWEEN_ALERT = 5  # minutes
LAST_ALERT = "last_alert.json"

FORMAT = '%(funcName)s %(asctime)s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

"""
 1) Extracting session
 2) Uploading to s3
 3) Check Threshold
 4) Invoke SNS
"""


def update_alert(now, s3_session):
    updated_alert = {"time": now.strftime("%Y/%m/%d %H:%M:%S")}
    s3_client.put_object(Bucket=S3_BUCKET,
                         Key=DATA + "/" + s3_session + "/" + LAST_ALERT,
                         Body=json.dunps(updated_alert))


def lambda_handler(event, context):
    logger.info("Received data from Arduino device, processing started.")

    current_timestamp = str(int(time.time()))
    # check if event is not empty
    if bool(event):
        error_message = "Error: JSON payload provided is empty, nothing to process"
    logger.error(error_message)
    raise Exception(error_message)

    logger.info("JSON payload contains data.")

    # extract date to elaborate session
    session = event[0]["time"]
    try:
        session_datetime = datetime.strptime(session, "%Y/%m/%d %H:%M:%S")
        s3_session = datetime.strftime(session_datetime, "%Y%m%d")
    except Exception:
        error_message = "Error: session is not formatted properly"
        logger.error(error_message)
        raise Exception(error_message)

    logger.info("Processing data for session {0}".format(s3_session))

    """# checking if we already have an s3 session
    is_session = False
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=DATA)
    for obj in response["Contents"]:
        if obj["Key"] == DATA + "/" + s3_session:
            logger.info("Session already exists")
            is_session = True
            break"""

    key = DATA + "/" + s3_session + "/" + current_timestamp + ".json"
    s3_client.put_object(Bucket=S3_BUCKET,
                         Key=key,
                         Body=json.dumps(event))

    for dp in event:
        temperature = float(dp["value"])

        if temperature < THRESHOLD:
            logger.info("BBQ Temperature below the threshold")
            # Checking last alert, we don't want to keep sending alert if one was just received few mins ago
            try:
                alert = json.loads(s3_client.get_object(Bucket=S3_BUCKET,
                                                        Key=DATA + "/" + s3_session + "/" + LAST_ALERT)[
                                       "Body"].read())
                last_alert = datetime.strptime(alert["time"],
                                               "%Y/%m/%d %H:%M:%S")
                now = datetime.now()
                if datetime.now() - last_alert > INTERVAL_BETWEEN_ALERT:
                    # time to alerting again
                    logger.info("Invoking SNS service")
                    update_alert(now, s3_session)
                    # TODO: adding SNS service
            except Exception:
                # we probably don't have a last_alert file yet (first time)
                logger.info("This is the first alert for the session")
                # TODO adding SNS service
                updated_alert(now, s3_session)

