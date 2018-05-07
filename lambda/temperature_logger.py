import json
import logging
import boto3
from datetime import datetime
import time
import numpy as np
import pandas as pd
from boto3 import resource
from boto3.dynamodb.conditions import Key

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
dynamodb_client = resource('dynamodb')

CONFIG_FILE = "bbq_config.json"
MESSAGE_SUBJECT = "Temperature below threshold"
MESSAGE = "Temperature value: {0}"
S3_BUCKET = "cleverbbq"

FORMAT = '%(funcName)s %(asctime)s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def update_alert(session_key, config, is_update):
    table = dynamodb_client.Table(config["alert-table"])
    new_alert = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    if is_update:
        table.update_item(Key={"session": session_key},
                          UpdateExpression="SET alert = :new_alert",
                          ExpressionAttributeValues={
                              ":new_alert": new_alert})
    else:
        table.put_item(Item={"session": session_key, "alert": new_alert})


def read_last_update(config, session_key):
    table = dynamodb_client.Table(config["alert-table"])
    response = table.get_item(Key={"session": session_key})

    if table.item_count == 0:
        logger.info("No alert yet")
        return ""
    else:
        last_alert = response["Item"]["alert"]
        return last_alert


def get_slope(config, session_key):
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=session_key)

    if response is not None:
        tmp_obj = response["Contents"]
        if len(tmp_obj) < config["records-to-consider"]:
            logger.info("Not enough data to calculate slope")
            return None

        temperature_logs = tmp_obj[config["records-to-consider"]:]
        temperature_frames = []
        for log in temperature_logs:
            s3_obj = json.loads(
                s3_client.get_object(Bucket=S3_BUCKET, Key=log["Key"])[
                    "Body"].read())
            temperature_frames.append(pd.DataFrame(s3_obj))

        resulting_df = pd.concat(temperature_frames)
        coeff = np.polyfit(resulting_df["value"].index.values,
                           list(resulting_df["value"]), 1)
        slope = coeff[-2]

    return float(slope)


def lambda_handler(event, context):
    logger.info("Received data from Arduino device, processing started.")

    # check if event is not empty
    if len(event) == 0:
        error_message = "Error: JSON payload provided is empty, nothing to process"
        logger.error(error_message)
        raise Exception(error_message)

    logger.info("JSON payload contains data.")

    logger.debug("Retrieving configurations")
    config = json.loads(
        s3_client.get_object(Bucket=S3_BUCKET, Key=CONFIG_FILE)["Body"].read())

    current_timestamp = str(int(time.time()))

    # extract date to elaborate session
    session = event[0]["time"]
    try:
        session_datetime = datetime.strptime(session, "%Y/%m/%d %H:%M:%S")
        s3_session = datetime.strftime(session_datetime, "%Y%m%d")
    except Exception:
        error_message = "Error: session is not formatted properly"
        logger.error(error_message)
        raise Exception(error_message)

    session_key = config["temperature-data"] + "/" + s3_session + "/"
    logger.info("Processing data for session {0}".format(s3_session))

    # sorting the temperature series here and uploading to s3
    sorted_event = sorted(event, key=lambda item: item["time"])
    s3_client.put_object(Bucket=S3_BUCKET,
                         Key=session_key + "log_" + current_timestamp + ".json",
                         Body=json.dumps(sorted_event))

    last_temperature_value = sorted_event[-1]
    lower_bound = config["lower-threshold"]
    upper_bound = config["upper-threshold"]
    last_alert = read_last_update(config, session_key)

    # this is to cover the beginning of the session
    if last_alert == "":
        update_alert(session_key, config, False)
        return

    # this is the core part of the script
    if lower_bound < last_temperature_value < upper_bound:
        now = datetime.now()
        if now - last_alert > config["alert-interval"]:
            logger.info(
                "BBQ Temperature below the threshold and last alert elapsed")

            logger.info("Checking temperature trend")
            slope = get_slope(config, session_key)
            if slope is None:
                logger.info("Too soon to calculate slope coefficient")
            elif slope < config["slope-coeff"]:
                logger.info("Invoking SNS service")
                sns_client.publish(TopicArn=config["sns-topic"],
                                   Message=MESSAGE.format(
                                       last_temperature_value),
                                   Subject=MESSAGE_SUBJECT)
                logger.info("Updating last alert value")
                update_alert(s3_session, config, True)
            else:
                logger.info("Temperature seems stable: {0}".format(slope))

    logger.info("Temperature data recorded")
    return {"status": "OK"}
