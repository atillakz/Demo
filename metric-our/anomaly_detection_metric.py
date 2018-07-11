#!/home/user/anaconda3/bin/python
from influxdb import InfluxDBClient, DataFrameClient
import json
import time
import datetime
import requests

i = 0
client_write = InfluxDBClient('192.168.4.33', 8086, 'root', 'root', 'Online_Classification')
client = InfluxDBClient('192.168.4.33', 8086, 'test', '12345', 'Labview')


def anomaly_detection(tag, measurement):
    rs_tag = client.query("SELECT {0} from {1} GROUP BY * ORDER BY DESC LIMIT 1;".format(tag, measurement))
    data1 = list(rs_tag.get_points())

    rs_check_for_status_engine = client.query(
        "SELECT unit_run from {0} GROUP BY * ORDER BY DESC LIMIT 1;".format(measurement))
    data2 = list(rs_check_for_status_engine.get_points())
    value_to_check = data2[0]["unit_run"]

    try:
        dict_data = data1[0]
    except Exception as e:
        print(e)
    tag_value = dict_data[tag]
    current_time = dict_data['time']

    tag_predicted = None
    string_of_query = str(tag + "_predicted")
    rs_tag_predicted = client_write.query(
        "SELECT {0} from {1} WHERE time > now() - 1m;".format(string_of_query, measurement))
    data2 = list(rs_tag_predicted.get_points())

    for i in range(len(data2)):
        if data2[i]['time'] == current_time:
            tag_predicted = data2[i][string_of_query]
            time_tag_predicted = data2[i]['time']

    if tag_predicted is None:
        tag_predicted = tag_value

    if (tag_value > tag_predicted * 1.05) or (tag_value < tag_predicted * 0.95):

        tag_anomaly_status = 1

    else:
        tag_anomaly_status = 0
    string_anomaly_json = str(tag+"_anomaly_status")
    json_tag_anom_status = [
        {
            "measurement": measurement,
            "time": current_time,
            "fields": {
                string_anomaly_json: tag_anomaly_status
            }
        }
    ]
    json_zero = [
        {
            "measurement": measurement,
            "time": current_time,
            "fields": {
                string_anomaly_json: 0
            }
        }
    ]

    if value_to_check == 1:
        client_write.write_points(json_tag_anom_status)
    else:
        client_write.write_points(json_zero)



while True:
    anomaly_detection("engine_pcd","unit1")
    anomaly_detection("ngp", "unit1")
    anomaly_detection("npt", "unit1")
    anomaly_detection("t1_temperature", "unit1")
    anomaly_detection("t5_average_temperature", "unit1")

    anomaly_detection("engine_pcd", "unit2")
    anomaly_detection("ngp", "unit2")
    anomaly_detection("npt", "unit2")
    anomaly_detection("t1_temperature", "unit2")
    anomaly_detection("t5_average_temperature", "unit2")

    anomaly_detection("engine_pcd", "unit3")
    anomaly_detection("ngp", "unit3")
    anomaly_detection("npt", "unit3")
    anomaly_detection("t1_temperature", "unit3")
    anomaly_detection("t5_average_temperature", "unit3")
    time.sleep(8)
