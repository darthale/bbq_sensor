import os
import json
import datetime
import time
import random
import boto3
import json

S3_BUCKET = "cleverbbq"
DATA = "temperature_data"
FILE_EXT = "json"

s3_client = boto3.client("s3")


def gen_test_temperature_data(start, interval, trend_type):
    current_timestamp = str(int(time.time()))
    file_name = "log_" + current_timestamp + ".json"

    start_dt = datetime.datetime.strptime(start, "%Y/%m/%d %H:%M:%S")
    events = []

    temperature = 51.8
    for i in range(0, interval):
        start_dt = start_dt + datetime.timedelta(seconds=10)
        if trend_type == "ASCENDING":
            temperature += 0.1
        elif trend_type == "DESCENDING":
            temperature -= 0.1
        else:
            temperature += 0

        event = {
            "time": datetime.datetime.strftime(start_dt, "%Y/%m/%d %H:%M:%S"),
            "value": temperature}
        events.append(event)

    print(event)

    f_path = "../dashboard/data/" + file_name
    with open(f_path, "w") as f_out:
        json.dump(events, f_out, indent=4)

    return file_name, events


def upload_to_s3_session(file_name, events):
    session_key = "20180507"
    s3_client.put_object(Bucket=S3_BUCKET,
                         Key=DATA + "/" + session_key + "/" + file_name,
                         Body=json.dumps(events))


file_name, events = gen_test_temperature_data("2018/05/07 11:12:00", 30, "")
upload_to_s3_session(file_name, events)
