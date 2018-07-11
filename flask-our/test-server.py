#!/home/user/anaconda3/bin/python
import os
from influxdb import DataFrameClient
import pandas as pd    #'0.22.0'
from sklearn.externals import joblib             #The scikit-learn version is 0.19.1.
from flask import Flask, jsonify, request         #'0.12.2'
from flask import render_template
import requests
import json
from datetime import datetime, timedelta
#source ~/venvs/flaskproj/bin/activate
import numpy as np

app = Flask(__name__)

predicted_values_for_linear_regression = [ 0, 0, 0]

list_of_users = list()
list_of_all_users = list()
##################################
dict_to_check_g_1 = dict()
dict_to_check_w_1 = dict()
dict_to_check_g_1['check'] = 0
dict_to_check_w_1['check'] = 0

dict_to_check_g_2 = dict()
dict_to_check_w_2 = dict()
dict_to_check_g_2['check'] = 0
dict_to_check_w_2['check'] = 0


dict_to_check_g_3 = dict()
dict_to_check_w_3 = dict()
dict_to_check_g_3['check'] = 0
dict_to_check_w_3['check'] = 0



###############################################
ngp_check = dict()

ngp_check['check'] = 0

#sudo docker run -it -p 9000:9000 --name tf-serve -v /home/user/Documents/tf_model/serve/:/serve/ epigramai/model-server:light --port=9000 --model_name=test --model_base_path=./serve/test


##############################!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!###############################

@app.route('/predict', methods=['POST'])

def apicall_ddd(responses2 = None):

    """API Call

    Pandas dataframe (sent as a payload) from API Call
    """
    try:

        test_json = request.get_json()

        test = pd.read_json(test_json, orient='records')

        #print(test)

        #print("Input variables:")

        #print(query_df)



    except Exception as e:

        raise e

   # clf = 'lin_reg_model.pkl'
    clf = 'lin_reg_model.pkl'


    if test.empty:

        return(bad_request())

    else:

        #Load the saved model
        #print("Loading the model...")

        lin_reg_model = None

        with open(clf,'rb') as f:

            lin_reg_model = joblib.load(f)

       # lin_reg_model = joblib.load('/home/q/new_project/models/kmeans_model.pkl')

        #print("The model has been loaded...doing predictions now...")

        predictions = lin_reg_model.predict(test)

        #print("Last predicted value: " ,predictions)

        predictions = np.round(predictions, 9)

        my_list = map(lambda x: x[0], predictions)



        """Add the predictions as Series to a new pandas dataframe
                                OR
           Depending on the use-case, the entire test data appended with the new files
        """
        prediction_series = list(pd.Series(my_list))

        final_predictions = pd.DataFrame(list(zip(prediction_series)))

        """We can be as creative in sending the responses.
           But we need to send the response codes as well.
        """
        responses2 = jsonify(predictions=final_predictions.to_json(orient="records"))

        #print(final_predictions.to_json(orient="records"))

        responses2.status_code = 200

        return (responses2)



#################################################!!!!!!!!!!!!!!!!!!!!!THE FRONTEND OF LINEAR REGRESSION!!!!!!!!!!!!!!!!!!!!!!!!!!!!############################################################

@app.route('/lin-reg', methods=['GET'])

def show_lin_reg():

    return render_template('linear_regression.html', predictions=predicted_values_for_linear_regression[len(predicted_values_for_linear_regression)-1:len(predicted_values_for_linear_regression)])



@app.route('/all-users', methods=['POST'])

def register1():

    """API Call

    Pandas dataframe (sent as a payload) from API Call
    """

    test_json = request.get_json()

    #test = pd.read_json(test_json, orient='records')

    #query_df = pd.DataFrame(test)

    loaded_r = json.loads(test_json)

    print(loaded_r)



    if loaded_r['Status'] == 1:

        if not loaded_r['User'] in list_of_all_users:
            list_of_all_users.append(loaded_r['User'])


    return "done"


@app.route('/users', methods=['POST'])

def register():

    """API Call

    Pandas dataframe (sent as a payload) from API Call
    """

    test_json = request.get_json()

    #test = pd.read_json(test_json, orient='records')

    #query_df = pd.DataFrame(test)

    loaded_r = json.loads(test_json)

    print(loaded_r)



    if loaded_r['Status'] == 1:

        if not loaded_r['User'] in list_of_users:

            list_of_users.append(loaded_r['User'])

    else:
        list_of_users.remove(loaded_r['User'])

    return "done"



@app.route('/show-u', methods=['GET'])

def users():
    return render_template('users.html', users = list_of_users)

###################################
@app.route('/predict2', methods=['POST'])

def apicall_two(responses2 = None):

    """API Call

    Pandas dataframe (sent as a payload) from API Call
    """
    try:

        test_json = request.get_json()

        test = pd.read_json(test_json, orient='records')

        #print(test)


        #print("Input variables:")

        #print(query_df)



    except Exception as e:

        raise e

    clf = 'lin_reg_model_two.pkl'

    if test.empty:

        return(bad_request())

    else:

        #Load the saved model
        #print("Loading the model...")

        lin_reg_model = None

        with open(clf,'rb') as f:

            lin_reg_model = joblib.load(f)

       # lin_reg_model = joblib.load('/home/q/new_project/models/kmeans_model.pkl')

        #print("The model has been loaded...doing predictions now...")

        predictions = lin_reg_model.predict(test)

        #print("Last predicted value: " ,predictions)

        predictions = np.round(predictions, 9)

        my_list = map(lambda x: x[0], predictions)

       #

        #print("Anomaly is :", armani)


        #predicted_values_for_linear_regression.append(predictions)




        """Add the predictions as Series to a new pandas dataframe
                                OR
           Depending on the use-case, the entire test data appended with the new files
        """
        prediction_series = list(pd.Series(my_list))

        final_predictions = pd.DataFrame(list(zip(prediction_series)))

        """We can be as creative in sending the responses.
           But we need to send the response codes as well.
        """
        responses2 = jsonify(predictions=final_predictions.to_json(orient="records"))

        #print(final_predictions.to_json(orient="records"))

        responses2.status_code = 200

        return (responses2)

###################################

@app.route('/predict3', methods=['POST'])

def apicall_three(responses2 = None):

    """API Call

    Pandas dataframe (sent as a payload) from API Call
    """
    try:

        test_json = request.get_json()

        test = pd.read_json(test_json, orient='records')

        #print(test)



        #print("Input variables:")

        #print(query_df)



    except Exception as e:

        raise e

    clf = 'lin_reg_model_three.pkl'

    if test.empty:

        return(bad_request())

    else:

        #Load the saved model
        #print("Loading the model...")

        lin_reg_model = None

        with open(clf,'rb') as f:

            lin_reg_model = joblib.load(f)

       # lin_reg_model = joblib.load('/home/q/new_project/models/kmeans_model.pkl')

        #print("The model has been loaded...doing predictions now...")

        predictions = lin_reg_model.predict(test)

        #print("Last predicted value: " ,predictions)

        predictions = np.round(predictions, 9)

        my_list = map(lambda x: x[0], predictions)


        #print("Anomaly is :", armani)


        #predicted_values_for_linear_regression.append(predictions)




        """Add the predictions as Series to a new pandas dataframe
                                OR
           Depending on the use-case, the entire test data appended with the new files
        """
        prediction_series = list(pd.Series(my_list))

        final_predictions = pd.DataFrame(list(zip(prediction_series)))

        """We can be as creative in sending the responses.
           But we need to send the response codes as well.
        """
        responses2 = jsonify(predictions=final_predictions.to_json(orient="records"))

        #print(final_predictions.to_json(orient="records"))

        responses2.status_code = 200

        return (responses2)

########################TELEGRAM##############################

@app.route('/ngp', methods=['POST'])
def apicall_ngp(responses2 = None):

    """API Call

    Pandas dataframe (sent as a payload) from API Call
    """
    try:

        test_json = request.get_json()

        test = pd.read_json(test_json, orient='records')

        #print(test)

        ngp = test



    except Exception as e:

        raise e


    if ngp_check['check'] < ngp:

        for k in list_of_users:

            response = requests.post(
                    url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                    data={'chat_id': k, 'text': "Повышенный расход топливного газа ГПА1"}).json()

    if ngp_check['check'] > ngp:

        for k in list_of_users:
                response = requests.post(
                    url='https://api.telegram.org/bot{0}/{1}'.format(
                        "550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                        "sendMessage"),
                    data={'chat_id': k, 'text': "Проблема решена по топливному газу ГПА1"}).json()

    ngp_check['check'] = ngp

    #print("Anomaly is :", ngp)

    return "OK"


@app.route('/anomaly/unit1', methods=['POST', 'GET'])
def anomaly_unit1():
    try:

        test_json = request.get_json()
        d = json.loads(test_json)
        type_an = d["type"]

    except Exception as e:

        raise e

    if type_an == "npt_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Повышенные обороты NPT, ГПА1"}).json()

    if type_an == "ngp_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Повышенные обороты NGP, ГПА1"}).json()

    if type_an == "engine_pcd_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Аномалия давления PCD, ГПА1"}).json()

    if type_an == "t1_temperature_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Аномалия температуры T1, ГПА1"}).json()

    if type_an == "t5_average_temperature_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Аномалия температуры T5, ГПА1"}).json()

    return "OK"









@app.route('/anomaly/unit2', methods=['POST', 'GET'])
def anomaly_unit2():
    try:

        test_json = request.get_json()
        d = json.loads(test_json)
        type_an = d["type"]

    except Exception as e:

        raise e

    if type_an == "npt_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Повышенные обороты NPT, ГПА2"}).json()

    if type_an == "ngp_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Повышенные обороты NGP, ГПА2"}).json()

    if type_an == "engine_pcd_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Аномалия давления PCD, ГПА2"}).json()

    if type_an == "t1_temperature_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Аномалия температуры T1, ГПА2"}).json()

    if type_an == "t5_average_temperature_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Аномалия температуры T5, ГПА2"}).json()

    return "OK"





@app.route('/anomaly/unit3', methods=['POST', 'GET'])
def anomaly_unit3():
    try:

        test_json = request.get_json()
        d = json.loads(test_json)
        type_an = d["type"]

    except Exception as e:

        raise e

    if type_an == "npt_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Повышенные обороты NPT, ГПА3"}).json()

    if type_an == "ngp_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Повышенные обороты NGP, ГПА3"}).json()

    if type_an == "engine_pcd_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Аномалия давления PCD, ГПА3"}).json()

    if type_an == "t1_temperature_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Аномалия температуры T1, ГПА3"}).json()

    if type_an == "t5_average_temperature_anomaly_status":
        for k in list_of_users:
            response = requests.post(
                url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                data={'chat_id': k, 'text': "Аномалия температуры T5, ГПА3"}).json()

    return "OK"


@app.route('/error/unit1', methods=['POST', 'GET'])
def error_unit1():

    try:
        test_json = request.get_json()

        test = pd.read_json(test_json, orient='records')

        gas_fuel_flow_status = pd.DataFrame(test['gas_fuel_flow_status'])

        waterwash_status  =   pd.DataFrame(test['waterwash_status'])

        if dict_to_check_g_1['check'] < gas_fuel_flow_status.values:

            for k in list_of_users:

                response = requests.post(
                        url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                        data={'chat_id': k, 'text': "Повышенный расход топливного газа ГПА1"}).json()

        if dict_to_check_g_1['check'] > gas_fuel_flow_status.values:

            for k in list_of_users:
                response = requests.post(
                    url='https://api.telegram.org/bot{0}/{1}'.format(
                        "550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                        "sendMessage"),
                    data={'chat_id': k, 'text': "Проблема решена по топливному газу ГПА1"}).json()

        dict_to_check_g_1['check'] = gas_fuel_flow_status.values


        if dict_to_check_w_1['check'] < waterwash_status.values:

            for k in list_of_users:

                response = requests.post(
                        url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                        data={'chat_id': k, 'text': "Наступление промывки для ГПА1"}).json()

        if dict_to_check_w_1['check'] > waterwash_status.values:

            for k in list_of_users:
                response = requests.post(
                    url='https://api.telegram.org/bot{0}/{1}'.format(
                        "550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                        "sendMessage"),
                    data={'chat_id': k, 'text': "Промывка по ГПА1 прошло успешно"}).json()

        dict_to_check_w_1['check'] = waterwash_status.values

    except Exception as e:

        raise e

    return 'OK'

@app.route('/error/unit2', methods=['POST', 'GET'])
def error_unit2():

    try:
        test_json = request.get_json()

        test = pd.read_json(test_json, orient='records')

        gas_fuel_flow_status = pd.DataFrame(test['gas_fuel_flow_status'])

        waterwash_status  =   pd.DataFrame(test['waterwash_status'])

        if dict_to_check_g_2['check'] < gas_fuel_flow_status.values:

            for k in list_of_users:

                response = requests.post(
                        url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                        data={'chat_id': k, 'text': "Повышенный расход топливного газа ГПА2"}).json()

        if dict_to_check_g_2['check'] > gas_fuel_flow_status.values:

            for k in list_of_users:
                response = requests.post(
                    url='https://api.telegram.org/bot{0}/{1}'.format(
                        "550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                        "sendMessage"),
                    data={'chat_id': k, 'text': "Проблема решена по топливному газу ГПА2"}).json()

        dict_to_check_g_2['check'] = gas_fuel_flow_status.values


        if dict_to_check_w_2['check'] < waterwash_status.values:

            for k in list_of_users:

                response = requests.post(
                        url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                        data={'chat_id': k, 'text': "Наступление промывки для ГПА2"}).json()

        if dict_to_check_w_2['check'] > waterwash_status.values:

            for k in list_of_users:
                response = requests.post(
                    url='https://api.telegram.org/bot{0}/{1}'.format(
                        "550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                        "sendMessage"),
                    data={'chat_id': k, 'text': "Промывка по ГПА2 прошло успешно"}).json()

        dict_to_check_w_2['check'] = waterwash_status.values

    except Exception as e:

        raise e

    return 'OK'

@app.route('/error/unit3', methods=['POST', 'GET'])
def error_unit3():

    try:
        test_json = request.get_json()

        test = pd.read_json(test_json, orient='records')

        gas_fuel_flow_status = pd.DataFrame(test['gas_fuel_flow_status'])

        waterwash_status  =   pd.DataFrame(test['waterwash_status'])

        if dict_to_check_g_3['check'] < gas_fuel_flow_status.values:

            for k in list_of_users:

                response = requests.post(
                        url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                        data={'chat_id': k, 'text': "Повышенный расход топливного газа ГПА3"}).json()

        if dict_to_check_g_3['check'] > gas_fuel_flow_status.values:

            for k in list_of_users:
                response = requests.post(
                    url='https://api.telegram.org/bot{0}/{1}'.format(
                        "550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                        "sendMessage"),
                    data={'chat_id': k, 'text': "Проблема решена по топливному газу ГПА3"}).json()

        dict_to_check_g_3['check'] = gas_fuel_flow_status.values


        if dict_to_check_w_3['check'] < waterwash_status.values:

            for k in list_of_users:

                response = requests.post(
                        url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                                 "sendMessage"),
                        data={'chat_id': k, 'text': "Наступление промывки для ГПА3"}).json()

        if dict_to_check_w_3['check'] > waterwash_status.values:

            for k in list_of_users:
                response = requests.post(
                    url='https://api.telegram.org/bot{0}/{1}'.format(
                        "550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                        "sendMessage"),
                    data={'chat_id': k, 'text': "Промывка по ГПА3 прошло успешно"}).json()

        dict_to_check_w_3['check'] = waterwash_status.values

    except Exception as e:

        raise e

    return 'OK'



@app.route('/start-telebot', methods=['POST', 'GET'])
def start_telebot():
    try:
        test_json = request.get_json()
        d = json.loads(test_json)
        status_telebot = d["Status"]

    except Exception as e:

        raise e

    for k in list_of_all_users:
        response = requests.post(
            url='https://api.telegram.org/bot{0}/{1}'.format("550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc",
                                                             "sendMessage"),
            data={'chat_id': k, 'text': "Server was started. Please /start it."}).json()

    return "OK"



if __name__ == '__main__':
    app.run(host="0.0.0.0")







