#!/home/user/anaconda3/bin/python
from apiclass import *

########___________FIELDS________________#############
real_rate_waterwash_last_value = dict()

real_rate_waterwash_last_value['last_value'] = 0.5

waterwash_check = dict()

waterwash_check['status'] = 0

last_value_loss_index = dict()

last_value_loss_index['last_value'] = np.array(15)

time_start_waterwash = dict()

time_start_waterwash['start'] = 0

time_finish_waterwash = dict()

time_finish_waterwash['finish'] = 0

vremya_alert_waterwash = float(0)

last_value_waterwash_rate = dict()

last_value_loss_index['last_value'] = np.array(0.25)

last_value_waterwash_rest_time = dict()

last_value_waterwash_rest_time['last_value'] = np.array(0.5)

########___________METHODS________________#############
#############MAIN_CHECK###################



################################

def index_loss(a, b):

    return (a/b) *100

def comparator_for_index(x):

    if x >= 119.0408:

        return 1

    else:

        return 0

def time_to_waterwash(y):

    return 2-y

def comparator_for_flow(data):

    if data > 0.06:

        return 1

    else:

        return 0

def difference(a, b):
    return (a - b)


def days_hours_minutes(td):
    days = td.days * 24
    hours = td.seconds / 3600
    total = days + hours
    return total.values

########The database to upload data from ML######################

host = '192.168.4.33'

port = 8086

user = ''

password = ''

db_name = 'Online_Classification' #metricdata

########The database to retrieve data from ML######################

zhost = '192.168.4.33'

zport = 8086

zuser = ''

zpassword = ''

zdb_name = 'Labview'

power_db_name = 'metricdata'

query_body = """
SELECT power_c,gas_fuel_flow, gas_fuel_flow_x,timestamp, waterwash_lasttime, unit_run

FROM unit2 ORDER BY time DESC LIMIT 1

"""

zquery_body = """
SELECT gas_fuel_flow_y

FROM unit2 ORDER BY time DESC LIMIT 1

"""
mean_rate_query_body = """SELECT mean("waterwash_rate") AS "mean_waterwash_rate" FROM unit2 WHERE time > now() - 2m """


power_e_writer_client = DataFrameClient(host,port,user, password, power_db_name)

client = DataFrameClient(host, port, user, password, db_name)

writer_client = DataFrameClient(host, port, user, password, db_name)

########___________!!!!!!!!PROCESS!!!!!!!!________________#############

starttime=time.time()

while True:

    try:
        predictor = Online_predictor(zhost, zport, zuser, zpassword, zdb_name,query_body)

        predictor.get_data_from_influx()

        predictor.read_data()

        df = predictor.prepare_data()

        df = df.set_index('index')

        status = df['unit_run'].values

        timestamp = df['timestamp']

        timestamp = timestamp.reset_index()

        timestamp = timestamp.set_index('timestamp')

        timestamp.index = pd.to_datetime(timestamp.index)

        a = str(timestamp.index)

        a = a[16:35]

        #print(status)

        if status == 1:

            data = client.query(zquery_body)

            new_data = dict(data)

            for i in new_data:
                my_data = new_data[i]

            my_data = my_data.reset_index()

            my_data = my_data.set_index('index')

            my_data = my_data.dropna()

            predicted_gas_fuel_flow = my_data['gas_fuel_flow_y']

            if df.index == my_data.index:

                waterwash_lasttime = df['waterwash_lasttime']

                waterwash_lasttime = waterwash_lasttime.reset_index()

                waterwash_lasttime = waterwash_lasttime.set_index('waterwash_lasttime')

                waterwash_lasttime.index = pd.to_datetime(waterwash_lasttime.index)

                dateandtime = datetime.now()

                timenow = pd.to_datetime(dateandtime)

                ggg = timenow - waterwash_lasttime.index

                waterwash_delta_time_now = days_hours_minutes(ggg)

                mownost_nagnetatelya = df['power_c']

                actual_gas_fuel_flow = df['gas_fuel_flow_x']

                gtg = df['gas_fuel_flow']

                turbine_power = gtg.values / predicted_gas_fuel_flow.values

                armani = comparator_for_flow(difference(predicted_gas_fuel_flow.values,actual_gas_fuel_flow.values))

                index_poteri = index_loss(mownost_nagnetatelya, turbine_power)

               # print("Pn: ", mownost_nagnetatelya.values)

                #print("Pgtu: ", turbine_power)

                #print("Relationship: ", index_poteri.values - 100)

                if index_poteri.values > 100.00:

                    poterya_mownosti = index_poteri.values - 100.00

                else:

                    try:
                        poterya_mownosti = np.array(0.0)

                    except:

                        pass


                waterwash = comparator_for_index(index_poteri.values)

                if waterwash_check['status'] < waterwash:

                    time_start_waterwash['start'] = my_data.index

                if waterwash_check['status'] == 1 and waterwash == 1:

                    time_start_waterwash['next_start'] = my_data.index

                    try:

                        vremya_alert_waterwash = days_hours_minutes(time_start_waterwash['next_start'] - time_start_waterwash['start'])

                        vremya_alert_waterwash = round(float(vremya_alert_waterwash),6)


                    except:

                        pass


                if  waterwash_check['status'] > waterwash:

                    time_finish_waterwash['finish'] = my_data.index

                    whole_time_waterwash_alert = days_hours_minutes(time_finish_waterwash['finish'] - time_start_waterwash['start'])

                    pd_whole_time_waterwash_alert = pd.DataFrame({'waterwash_time': whole_time_waterwash_alert}, index=my_data.index)

                    upload_whole_time_waterwash_alert = writer_client.write_points(pd_whole_time_waterwash_alert, 'unit2')

                    vremya_alert_waterwash = float(0)

                waterwash_check['status'] = waterwash

                acc_loss_index = difference(poterya_mownosti, last_value_loss_index['last_value'])

                if acc_loss_index == 0 or acc_loss_index < 0:

                    acc_loss_index = last_value_waterwash_rate['last_value']

                last_value_waterwash_rate['last_value'] = acc_loss_index

                last_value_loss_index['last_value'] = poterya_mownosti

                if poterya_mownosti > 0:

                    real_rate_waterwash = poterya_mownosti / waterwash_delta_time_now

                    real_rate_waterwash_last_value['last_value'] = real_rate_waterwash

                elif poterya_mownosti <= 0:

                    real_rate_waterwash = real_rate_waterwash_last_value['last_value']



                if waterwash == 0 and waterwash_delta_time_now > 0:

                    waterwash_rest_time = (20 - poterya_mownosti) / real_rate_waterwash

                    last_value_waterwash_rest_time['last_value'] = waterwash_rest_time

                elif waterwash == 1:

                    waterwash_rest_time = np.array(0.0)




                data_to_influx = pd.DataFrame({'power_e':turbine_power,

                                               'loss_index': poterya_mownosti,

                                               'waterwash_status': waterwash,

                                               'gas_fuel_flow_status': armani,

                                               'waterwash_resttime': waterwash_rest_time,

                                               'waterwash_rate': acc_loss_index,

                                               'timestamp': a,

                                               'waterwash_time': waterwash_delta_time_now


                                               }, index=df.index)


                try:

                    upload_data_to_influx = writer_client.write_points(data_to_influx, ' unit2')

                    print("UNIT_2: Connected")


                except:



                    print("UNIT_2: Imitation model is not working")


                data_to_influx_power_e = pd.DataFrame({'power_e': turbine_power}, index=df.index)

                waterwash_over_time = pd.DataFrame({'waterwash_over_time': vremya_alert_waterwash}, index=df.index)

                upload_waterwash_over_time = writer_client.write_points(waterwash_over_time, ' unit2')

                upload_power_e = power_e_writer_client.write_points(data_to_influx_power_e, 'unit2')

        else:

           # data_to_influx = pd.DataFrame({'power_e': np.array(0.0),

               #                            'loss_index': np.array(0.0),

                 #                          'waterwash_resttime': last_value_waterwash_rest_time['last_value'],

                  #                         'timestamp': a,

                  #                         'waterwash_over_time': last_value_vremya_alert_waterwash['last_v']

                               #            }, index=df.index)

           # upload_data_to_influx = writer_client.write_points(data_to_influx, ' unit2')

            print("The UNIT 2 is not RUNNING")


    except:

        print("UNIT_2: DATABASE NOT FOUND")


    time.sleep(5.0 - ((time.time() - starttime) % 5.0))











