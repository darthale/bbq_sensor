import json
from datetime import datetime
import numpy as np
import pandas as pd

event = [
    {"time": "2018-04-19 10:00:00", "value": 55},
    {"time": "2018-04-19 10:10:00", "value": 29},
    {"time": "2018-04-19 10:03:00", "value": 25},
    {"time": "2018-04-19 10:04:00", "value": 28},
    {"time": "2018-04-19 10:05:00", "value": 28},
    {"time": "2018-04-19 10:14:00", "value": 30},
    {"time": "2018-04-19 10:12:00", "value": 30},
    {"time": "2018-04-19 10:11:00", "value": 25},
    {"time": "2018-04-19 10:09:00", "value": 29},
    {"time": "2018-04-19 10:01:00", "value": 26},
    {"time": "2018-04-19 10:03:00", "value": 27},
    {"time": "2018-04-19 10:02:00", "value": 27}]

sorted_data = sorted(event, key=lambda item: item["time"])

print (sorted_data)


def trendline(data, order=1):
    coeffs = np.polyfit(data.index.values, list(data), order)
    slope = coeffs[-2]
    return float(slope)


df = pd.DataFrame(sorted_data)
slope = trendline(df['value'])
print (slope)