#!/home/user/anaconda3/bin/python
import json
import requests

header = {'Content-Type': 'application/json', \
                  'Accept': 'application/json'}

import pandas as pd

import pandas as pd
from influxdb import DataFrameClient
from sklearn.externals import joblib
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import time
import numpy as np
##################################################################
host = '192.168.4.33'

port = 8086

user = ''

password = ''

db_name = 'Online_Classification'

writer_client = DataFrameClient(host, port, user, password, db_name)
###################################################################


c_db_name = 'Labview'

checker_client = DataFrameClient(host, port, user, password, c_db_name)

def anomaly_detection(measurement, url):
    db_data = writer_client.query(
        "SELECT gas_fuel_flow_status,waterwash_status from {0} GROUP BY * ORDER BY DESC LIMIT 1;".format(measurement))

    dict_db_data = dict(db_data)

    global mean_data

    for k in dict_db_data:
        mean_data = dict_db_data[k]

    mean_data = mean_data.reset_index()

    mean_data = mean_data.set_index('index')

    mean_data = mean_data.dropna()

    data = mean_data.to_json(orient='records')

    resp = requests.post(" http://192.168.1.37:5000/error/{0}".format(url), \
                         data=json.dumps(data), \
                         headers=header)


def checker(measurement):

    df = checker_client.query(
        "SELECT unit_run from {0} ORDER BY DESC LIMIT 1;".format(measurement))

    dict_df = dict(df)

    global check_data

    for k in dict_df:
        check_data = dict_df[k]

    check_data = check_data.reset_index()

    status = check_data['unit_run'].values

    return status






while True:

    if checker("unit1") == 1:

        anomaly_detection( "unit1","unit1" )

    if checker("unit2") == 1:
        anomaly_detection( "unit2","unit2")

    if checker("unit3") == 1:
         anomaly_detection( "unit3","unit3")

    time.sleep(6)



