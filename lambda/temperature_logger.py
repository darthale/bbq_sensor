import json
import logging
import boto3
from datetime import datetime
import time
import numpy as np
import pandas as pd

s3_client = boto3.client("s3")
sns_client = boto3.client('sns')

S3_BUCKET = "cleverbbq"
DATA = "temperature_data"
FILE_EXT = "json"
THRESHOLD = 30  # celsius
INTERVAL_BETWEEN_ALERT = 5  # minutes
LAST_ALERT = "last_alert.json"
MESSAGE_SUBJECT = "Temperature below threshold"
MESSAGE = "Temperature value: {0}"
SNS_TOPIC = ""

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


def is_trend_decreasing(temperature_series):
    """
    If the last value recorded by arduino is below the threshold,
    but the temperature is increasing, we don't send an alert
    :param event: json with record temperature
    :return: True if temperature is decreasing, False otherwise
    """

    df = pd.DataFrame(temperature_series)
    coeffs = np.polyfit(df["value"].index.values, list(df["value"]), 1)
    slope = coeffs[-2]

    if float(slope) >= 0:
        return False
    else:
        return True


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

    # sorting the temperature series here
    sorted_event = sorted(event, key=lambda item: item["time"])

    session_key = DATA + "/" + s3_session + "/"
    s3_client.put_object(Bucket=S3_BUCKET,
                         Key=session_key + current_timestamp + ".json",
                         Body=json.dumps(sorted_event))

    decreasing = is_trend_decreasing(sorted_event)
    last_temperature_value = sorted_event[-1]

    # temperature below threshold and decreasing trend
    if last_temperature_value < THRESHOLD and decreasing:
        logger.info("BBQ Temperature below the threshold")
        # Checking last alert, we don't want to keep sending alert if one was just received few mins ago
        try:
            alert = json.loads(s3_client.get_object(Bucket=S3_BUCKET,
                                                    Key=session_key + LAST_ALERT)[
                                   "Body"].read())
            last_alert = datetime.strptime(alert["time"],
                                           "%Y/%m/%d %H:%M:%S")
            now = datetime.now()
            if datetime.now() - last_alert > INTERVAL_BETWEEN_ALERT:
                # time to alerting again
                logger.info("Invoking SNS service")
                update_alert(now, s3_session)
                sns_client.publish(TopicArn=SNS_TOPIC,
                                   Message=MESSAGE.format(temperature),
                                   Subject=MESSAGE_SUBJECT)

        except Exception:
            # we probably don't have a last_alert file yet (first time)
            logger.info("This is the first alert for the session")
            sns_client.publish(TopicArn=SNS_TOPIC,
                               Message=MESSAGE.format(temperature),
                               Subject=MESSAGE_SUBJECT)
            update_alert(now, s3_session)
