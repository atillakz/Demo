#!/home/user/anaconda3/bin/python
import warnings
import itertools
import pandas as pd
import numpy as np
import statsmodels.api as sm
import statsmodels
import matplotlib.pyplot as plt
from influxdb import InfluxDBClient, DataFrameClient
import json
import time
import datetime
import requests
i=0
client = InfluxDBClient('192.168.4.33', 8086, 'test', '12345', 'Online_Classification')
client2 = InfluxDBClient('192.168.4.33', 8086, 'test', '12345', 'Labview')
dict_check = dict()

dict_check["engine_pcd_anomaly_status_unit1"] = 0
dict_check["ngp_anomaly_status_unit1"] = 0
dict_check["npt_anomaly_status_unit1"] = 0
dict_check["t5_average_temperature_anomaly_status_unit1"] = 0
dict_check["t1_temperature_anomaly_status_unit1"] = 0

dict_check["engine_pcd_anomaly_status_unit2"] = 0
dict_check["ngp_anomaly_status_unit2"] = 0
dict_check["npt_anomaly_status_unit2"] = 0
dict_check["t5_average_temperature_anomaly_status_unit2"] = 0
dict_check["t1_temperature_anomaly_status_unit2"] = 0

dict_check["engine_pcd_anomaly_status_unit3"] = 0
dict_check["ngp_anomaly_status_unit3"] = 0
dict_check["npt_anomaly_status_unit3"] = 0
dict_check["t5_average_temperature_anomaly_status_unit3"] = 0
dict_check["t1_temperature_anomaly_status_unit3"] = 0

def anomaly_detection_telegram(tag, measurement):
    rs_tag = client.query("SELECT {0} from {1} GROUP BY * ORDER BY DESC LIMIT 1;".format(tag, measurement))
    data1 = list(rs_tag.get_points())
    string_check = str(tag+"_"+measurement)

    try:
        dict_data = data1[0]
        global value
        value = dict_data[tag]

    except:
        print("Value of ", tag,"-",measurement, " does not exist in influxdb!!!")
        pass


    if value > dict_check[string_check]:
        header = {'Content-Type': 'application/json',

                  'Accept': 'application/json'}

        data1 = {}

        data1['type'] = tag

        data2 = json.dumps(data1)

        try:
            response = requests.post(
                url='http://localhost:5000/anomaly/{0}'.format(measurement),
                data=json.dumps(data2), headers=header)
            print("OOKKK")

        except Exception as e:
            print(e)
    dict_check[string_check] = value


while True:
    try:
        anomaly_detection_telegram("engine_pcd_anomaly_status", "unit1")
        anomaly_detection_telegram("ngp_anomaly_status", "unit1")
        anomaly_detection_telegram("npt_anomaly_status", "unit1")
        anomaly_detection_telegram("t5_average_temperature_anomaly_status", "unit1")
        anomaly_detection_telegram("t1_temperature_anomaly_status", "unit1")

        anomaly_detection_telegram("engine_pcd_anomaly_status", "unit2")
        anomaly_detection_telegram("ngp_anomaly_status", "unit2")
        anomaly_detection_telegram("npt_anomaly_status", "unit2")
        anomaly_detection_telegram("t5_average_temperature_anomaly_status", "unit2")
        anomaly_detection_telegram("t1_temperature_anomaly_status", "unit2")

        anomaly_detection_telegram("engine_pcd_anomaly_status", "unit3")
        anomaly_detection_telegram("ngp_anomaly_status", "unit3")
        anomaly_detection_telegram("npt_anomaly_status", "unit3")
        anomaly_detection_telegram("t5_average_temperature_anomaly_status", "unit3")
        anomaly_detection_telegram("t1_temperature_anomaly_status", "unit3")
    except:
        pass




    time.sleep(8)








