import boto3
import json
import numpy as np
import pandas as pd

S3_BUCKET = "cleverbbq"
DATA = "temperature_data"
FILE_EXT = "json"

s3_client = boto3.client("s3")


def parse_log_and_get_slope():
    session_key = "20180507"
    resp = s3_client.list_objects_v2(Bucket="cleverbbq",
                                     Prefix=DATA + "/" + session_key)

    temp_obj = resp["Contents"]
    sorted(temp_obj, key=lambda item: item["LastModified"])
    frames = []
    for obj in temp_obj:
        event = json.loads(
            s3_client.get_object(Bucket=S3_BUCKET, Key=obj["Key"])[
                "Body"].read())

        sorted_event = sorted(event, key=lambda item: item["time"])
        for val in sorted_event:
            print(val["time"] + "," + str(val["value"]))

        frames.append(pd.DataFrame(sorted_event))

    res = pd.concat(frames)
    coeffs = np.polyfit(res["value"].index.values, list(res["value"]), 1)
    slope = coeffs[-2]
    print(slope)


parse_log_and_get_slope()
