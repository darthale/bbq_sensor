import json
import logging
import boto3
from datetime import datetime
import time
import numpy as np
import pandas as pd

s3_client = boto3.client("s3")
sns_client = boto3.client('sns')

CONFIG_FILE = "bbq_config.json"
MESSAGE_SUBJECT = "Temperature below threshold"
MESSAGE = "Temperature value: {0}"

FORMAT = '%(funcName)s %(asctime)s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def update_alert(now, s3_session, config):
    updated_alert = {"time": now.strftime("%Y/%m/%d %H:%M:%S")}
    s3_client.put_object(Bucket=config["s3-bucket"],
                         Key=config[
                                 "temperature-data"] + "/" + s3_session + "/" +
                             config["last-alert-file"],
                         Body=json.dunps(updated_alert))


def is_trend_stable(temperature_series, stability_range):
    """
    Elaborates temperature trend
    :param event: json with record temperature
    :return: True if trend is withing stability range, False otherwise
    """

    df = pd.DataFrame(temperature_series)
    coeffs = np.polyfit(df["value"].index.values, list(df["value"]), 1)
    slope = coeffs[-2]

    if 0 <= float(slope) <= stability_range:
        return True
    else:
        return False


def lambda_handler(event, context):
    logger.info("Received data from Arduino device, processing started.")
    logger.debug("Retrieving configurations")

    config = json.loads(
        s3_client.get_object(Bucket="cleverbbq",
                             Key=CONFIG_FILE)["Body"].read())
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

    # sorting the temperature series here and uploading to s3
    sorted_event = sorted(event, key=lambda item: item["time"])
    session_key = config["temperature-data"] + "/" + s3_session + "/"
    s3_client.put_object(Bucket=config["s3-bucket"],
                         Key=session_key + current_timestamp + ".json",
                         Body=json.dumps(sorted_event))

    # temperature below threshold and decreasing trend
    stable = is_trend_stable(sorted_event, config["stability-range"])
    last_temperature_value = sorted_event[-1]
    lower_thres = config["lower-threshold"]
    upper_thres = config["upper-threshold"]

    if (lower_thres < last_temperature_value < upper_thres) and not stable:

        logger.info("BBQ Temperature below the threshold and trend not stable")
        # We don't want to keep sending alert if one was just received few mins ago
        try:
            # this contains the datetime of the last alert sent
            alert = json.loads(s3_client.get_object(Bucket=config["s3-bucket"],
                                                    Key=session_key + config[
                                                        "last-alert-file"])[
                                   "Body"].read())
            last_alert = datetime.strptime(alert["time"], "%Y/%m/%d %H:%M:%S")
            now = datetime.now()
            # if enough time elapsed between alerts
            if datetime.now() - last_alert > config["alert-interval"]:
                # time to alerting again
                logger.info("Invoking SNS service")
                update_alert(now, s3_session, config)
                sns_client.publish(TopicArn=config["sns-topic"],
                                   Message=MESSAGE.format(
                                       last_temperature_value),
                                   Subject=MESSAGE_SUBJECT)
        except Exception:
            # we don't have a last_alert file yet (first time)
            logger.info("This is the first alert for the session")
            sns_client.publish(TopicArn=config["sns-topic"],
                               Message=MESSAGE.format(last_temperature_value),
                               Subject=MESSAGE_SUBJECT)
            update_alert(now, s3_session, config)

    logger.info("Temperature data recorded")
