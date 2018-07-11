#!/home/user/anaconda3/bin/python
from apiclass import *


query_body = """
SELECT  npt, ngp,t5_average_temperature,engine_pcd ,t1_temperature, turb_air_inlet_filter_dp, unit_run

FROM unit2 ORDER BY time DESC LIMIT 1

"""

last_predicted = dict()


last_predicted['last_value'] = 0

########The database to retrieve data from ML######################

zhost = '192.168.4.33'

zport = 8086

zuser = ''

zpassword = ''

#zdb_name = 'Labview'

zdb_name = 'Labview'

db_name = 'Online_Classification'


writer_client = DataFrameClient(zhost, zport, zuser, zpassword, db_name)

starttime=time.time()

while True:

    try:

        predictor = Online_predictor(zhost, zport, zuser, zpassword, zdb_name,query_body)

        predictor.get_data_from_influx()

        predictor.read_data()

        df = predictor.prepare_data()

        status = df['unit_run'].values

        if status == 1:

                df = df.set_index('index')

                df.drop('unit_run', axis = 1, inplace=True)

                data = df.to_json(orient='records')


                try:

                    resp = requests.post(" http://192.168.1.37:5000/predict2", \
                                     data=json.dumps(data), \
                                     headers=header)

                    resp.status_code

                except:

                    print("UNIT_1: Web service is not WORKING")

                json_clasified = resp.json()

                for k in json_clasified:
                    needed_data = json_clasified[k]

                #print(needed_data)

                try:
                    final_data = float(needed_data[6:15])

                    last_predicted['last_value'] = final_data

                except:

                    final_data = last_predicted['last_value']

                #print('Predicted from model: ', final_data)

                #print('Actual value of gas flow: ', predictor.show())

                #print("Unit 1 (apitest) is RUNNING!")


                #print("Timestamp : " , timestamp.index)
                #print("Influx index: ", df.index)


                predicted_udelnyiRashodGaza = pd.DataFrame({'gas_fuel_flow_y': final_data

                                                           }, index=df.index)

                upload_udelnyi_rashod_gaza = writer_client.write_points(predicted_udelnyiRashodGaza, 'unit2')

                print("UNIT_2: CONNECTED")
        else:

            print("UNIT_2 is not RUNNING")

    except:

        print("UNIT_2: DATABASE NOT FOUND!!!")

        pass

    time.sleep(4.0 - ((time.time() - starttime) % 4.0))












