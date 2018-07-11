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
i = 0
client_write = InfluxDBClient('192.168.4.33', 8086, 'root', 'root', 'Online_Classification')
client = InfluxDBClient('192.168.4.33', 8086, 'test', '12345', 'Labview')



# Filter data
def convert_to_df_second(data):
    main_d = dict()
    for i in range(len(data)):
        main_d[list(data[i].values())[1]] = list(data[i].values())[0]
    data1 = pd.Series(main_d)
    print(data1)
    df = data1.to_frame()
    df = df.reset_index()
    df.columns = ['date', 'value']
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df


def convert_to_df_first(data):
    main_d = dict()
    for i in range(len(data)):
        main_d[list(data[i].values())[0]] = list(data[i].values())[1]
    data1 = pd.Series(main_d)
    print(data1)
    df = data1.to_frame()
    df = df.reset_index()
    df.columns = ['date', 'value']
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    return df



def add_ten_seconds(needed_time):
    needed_time_1 =  pd.to_datetime(needed_time)
    needed_time_2 = needed_time_1 + datetime.timedelta(0,10)
    return pd.tslib.Timestamp(needed_time_2)


results_engine_pcd_unit1 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('pcd_sarimax_model.pkl')
results_npt_unit1 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('npt_sarimax_model.pkl')
results_t1_temperature_unit1 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('t1_sarimax_model.pkl')
results_t5_average_temperaure_unit1 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('t5_sarimax_model.pkl')
results_ngp_unit1 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('ngp_sarimax_model.pkl')

results_engine_pcd_unit2 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('pcd_sarimax_model_2.pkl')
results_npt_unit2 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('npt_sarimax_model_2.pkl')
results_t1_temperature_unit2 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('t1_sarimax_model_2.pkl')
results_t5_average_temperaure_unit2 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('t5_sarimax_model_2.pkl')
results_ngp_unit2 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('ngp_sarimax_model_2.pkl')

results_engine_pcd_unit3 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('pcd_sarimax_model_3.pkl')
results_npt_unit3 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('npt_sarimax_model_3.pkl')
results_t1_temperature_unit3 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('t1_sarimax_model_3.pkl')
results_t5_average_temperaure_unit3 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('t5_sarimax_model_3.pkl')
results_ngp_unit3 = statsmodels.tsa.statespace.sarimax.SARIMAXResults.load('ngp_sarimax_model_3.pkl')

time_engine_pcd_unit1 = dict()
time_ngp_unit1 = dict()
time_npt_unit1 = dict()
time_t1_temperature_unit1 = dict()
time_t5_average_temperature_unit1 = dict()

time_engine_pcd_unit2 = dict()
time_ngp_unit2 = dict()
time_npt_unit2 = dict()
time_t1_temperature_unit2 = dict()
time_t5_average_temperature_unit2 = dict()

time_engine_pcd_unit3 = dict()
time_ngp_unit3 = dict()
time_npt_unit3 = dict()
time_t1_temperature_unit3 = dict()
time_t5_average_temperature_unit3 = dict()
time.sleep(2)




def sarimax_level(tag, measurement, sarimax_model, time_calc):

    rs_tag = client.query('SELECT {0} from {1} WHERE time > now() - 30m;'.format(tag,measurement))
    data1 = list(rs_tag.get_points())

    if list(data1[0].keys())[1] == "time":
        df_tag = convert_to_df_second(data1)
    else:
        df_tag = convert_to_df_first(data1)

    current_time = pd.to_datetime(df_tag.index.values[-1])
    my_mod_tag = sm.tsa.SARIMAX(df_tag.astype(float), order=(1, 1, 1),
                                enforce_stationarity=False,
                                enforce_invertibility=False)

    res_tag = my_mod_tag.filter(sarimax_model.params)

    insample_tag = res_tag.predict(start=len(df_tag.index), end=len(df_tag.index) + 6)

    time_calc[0] = current_time

    for i in range(5):
        time_calc[i + 1] = add_ten_seconds(time_calc[i])

    for i in range(6):
        json_influx = [
            {
                "measurement": measurement,
                "time": time_calc[i],
                "fields": {
                    str(tag+"_low_limit"): insample_tag.values[i] * 0.95,
                    str(tag + "_high_limit"): insample_tag.values[i] * 1.05,
                    str(tag + "_predicted"): insample_tag.values[i]
                }
            }
        ]
        client_write.write_points(json_influx)
        # warnings.warn("deprecated", DeprecationWarning)



# with warnings.catch_warnings():
#     warnings.simplefilter("ignore")
#     sarimax_level()


if __name__ == "__main__":
    while True:
        sarimax_level("engine_pcd", "unit1", results_engine_pcd_unit1, time_engine_pcd_unit1)
        sarimax_level("npt", "unit1", results_npt_unit1, time_npt_unit1 )
        sarimax_level("ngp", "unit1", results_ngp_unit1, time_ngp_unit1)
        sarimax_level("t1_temperature", "unit1", results_t1_temperature_unit1, time_t1_temperature_unit1)
        sarimax_level("t5_average_temperature", "unit1", results_t5_average_temperaure_unit1, time_t5_average_temperature_unit1)

        sarimax_level("engine_pcd", "unit2", results_engine_pcd_unit2, time_engine_pcd_unit2)
        sarimax_level("npt", "unit2", results_npt_unit2, time_npt_unit2)
        sarimax_level("ngp", "unit2", results_ngp_unit2, time_ngp_unit2)
        sarimax_level("t1_temperature", "unit2", results_t1_temperature_unit2, time_t1_temperature_unit2)
        sarimax_level("t5_average_temperature", "unit2", results_t5_average_temperaure_unit2, time_t5_average_temperature_unit2)

        sarimax_level("engine_pcd", "unit3", results_engine_pcd_unit3, time_engine_pcd_unit3)
        sarimax_level("npt", "unit3", results_npt_unit3, time_npt_unit3)
        sarimax_level("ngp", "unit3", results_ngp_unit3, time_ngp_unit3)
        sarimax_level("t1_temperature", "unit3", results_t1_temperature_unit3, time_t1_temperature_unit3)
        sarimax_level("t5_average_temperature", "unit3", results_t5_average_temperaure_unit3, time_t5_average_temperature_unit3)
        time.sleep(20)














