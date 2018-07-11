#!/home/user/anaconda3/bin/python
# -*- coding: utf-8 -*-
import telebot
from telebot import types
from influxdb import InfluxDBClient, DataFrameClient
import time
import numpy as np
import requests
import json
import datetime




token = "550975271:AAEXbwI63saLUWdXanZbn8KKDyu-UOGgmTc"

bot = telebot.TeleBot(token, threaded=False)

## Iniatialization of keyboard markups
markup_menu_1 = types.ReplyKeyboardMarkup(row_width=1)
markup_menu_2 = types.ReplyKeyboardMarkup(row_width=1)
markup_menu_3 = types.ReplyKeyboardMarkup(row_width=1)
markup_menu_3_1 = types.ReplyKeyboardMarkup(row_width=1)
markup_menu_4 = types.ReplyKeyboardMarkup(row_width=1)
markup_menu_5 = types.ReplyKeyboardMarkup(row_width=1)
markup_menu_5_1 = types.ReplyKeyboardMarkup(row_width=1)
markup_menu_6 = types.ReplyKeyboardMarkup(row_width=1)
markup_menu_7 = types.ReplyKeyboardMarkup(row_width=1)
markup_menu_7_1 = types.ReplyKeyboardMarkup(row_width=1)

## Buttons
btn_statistics = types.KeyboardButton('Cтатистика')
btn_otchet = types.KeyboardButton('Отчет')
btn_anomalii = types.KeyboardButton('Аномалии')
btn_ks_karaozek_2 = types.KeyboardButton('КС Караозек')
btn_nazad_to_1 = types.KeyboardButton('Назад')
btn_nazad_to_2 = types.KeyboardButton('Назад')
btn_nazad_to_3= types.KeyboardButton('Назад')
btn_nazad_to_4 = types.KeyboardButton('Назад')
btn_nazad_to_5= types.KeyboardButton('Назад')
btn_nazad_to_6 = types.KeyboardButton('Назад')
btn_nazad_to_7= types.KeyboardButton('Назад')
btn_vnachalo = types.KeyboardButton('В начало')
btn_stancia_3 = types.KeyboardButton('Станция')
btn_gpa1_3 = types.KeyboardButton('ГПА 1')
btn_gpa2_3 = types.KeyboardButton('ГПА 2')
btn_gpa3_3 = types.KeyboardButton('ГПА 3')
btn_stancia = types.KeyboardButton('Станция')
btn_ks_karaozek_4 = types.KeyboardButton('КС Караозек')
btn_sutki_5 = types.KeyboardButton('Час')
btn_nedelia_5 = types.KeyboardButton('Сутки')
btn_mesiac_5 = types.KeyboardButton('Неделя')
btn_god_5 = types.KeyboardButton('Месяц')
btn_da_6 = types.KeyboardButton('Да')
btn_net_6 = types.KeyboardButton('Нет')
btn_ks_karaozek_7 = types.KeyboardButton('КС Караозек')
btn_otkaz_podpisok_7_1 = types.KeyboardButton('Отказаться от всех подписок')

## Markup menu
markup_menu_1.add(btn_statistics, btn_otchet, btn_anomalii)
markup_menu_2.add(btn_ks_karaozek_2, btn_nazad_to_1)
markup_menu_3.add(btn_stancia_3, btn_gpa1_3, btn_gpa2_3, btn_gpa3_3, btn_nazad_to_2, btn_vnachalo)
markup_menu_3_1.add(btn_nazad_to_3, btn_vnachalo)
markup_menu_4.add(btn_ks_karaozek_4, btn_nazad_to_1)
markup_menu_5.add(btn_sutki_5, btn_nedelia_5, btn_mesiac_5, btn_god_5, btn_nazad_to_4, btn_vnachalo)
markup_menu_5_1.add(btn_nazad_to_5, btn_vnachalo)
markup_menu_6.add(btn_da_6, btn_net_6, btn_otkaz_podpisok_7_1)
markup_menu_7.add(btn_ks_karaozek_7, btn_nazad_to_6, btn_vnachalo)
markup_menu_7_1.add(btn_nazad_to_7, btn_vnachalo)


user_step = {}
###############################
print("Telegram bot is running!")
bot.send_message(350191272, "Server was started. Please /start it.")
header = {'Content-Type': 'application/json',
                  'Accept': 'application/json'}
data1 = dict()
data1['Status'] = 1
data2 = json.dumps(data1)

try:
    response = requests.post(
    url='http://localhost:5000/start-telebot',
    data=json.dumps(data2), headers=header)
except:
    print('Print host can not connect')
    pass


###############################


def get_user_step(cid):
    if cid in user_step:
        return user_step[cid]
    else:
        user_step[cid] = 1
        return




@bot.message_handler(commands=["start"])
def keyboard (message):
    cid = message.chat.id
    user_step[cid] = 1

    header = {'Content-Type': 'application/json',
              'Accept': 'application/json'}
    data3 = dict()
    data3['User'] = cid
    data3['Status'] = 1
    data4 = json.dumps(data3)
    try:
        response = requests.post(
            url='http://localhost:5000/all-users',
            data=json.dumps(data4), headers=header)
    except:
        print('Print host can not connect')
        pass

    bot.send_message(message.chat.id, "Выберите действие",reply_markup=markup_menu_1)


## Menu when user choose title from main menu
@bot.message_handler(func=lambda message:get_user_step(message.chat.id)==1)
def main_menu(message):
    cid = message.chat.id
    if message.text == "Cтатистика":
        user_step[cid] = 2
        bot.send_message(message.chat.id, "Выберите КС", reply_markup=markup_menu_2)
    elif message.text == "Отчет":
        user_step[cid] = 4
        bot.send_message(message.chat.id,"Выберите КС",reply_markup=markup_menu_4)
    elif message.text == "Аномалии":
        user_step[cid] = 6
        bot.send_message(message.chat.id,"Желаете ли Вы подписаться на уведомление аномалий?",reply_markup=markup_menu_6)




## Menu when user choose statistics
@bot.message_handler(func=lambda message:get_user_step(message.chat.id)==2)
def main_menu(message):
    cid = message.chat.id
    if message.text == "КС Караозек":
        user_step[cid] = 3
        bot.send_message(message.chat.id, "Выберите объект", reply_markup=markup_menu_3)
    elif message.text == "Назад":
        user_step[cid] = 1
        bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup_menu_1)



## Menu when user choose statistics/ks karaozek
@bot.message_handler(func=lambda message:get_user_step(message.chat.id)==3)
def main_menu(message):
    cid = message.chat.id
    client = InfluxDBClient('192.168.4.33', 8086, 'test', '12345', 'Labview')
    client2 = InfluxDBClient('192.168.4.33', 8086, 'test', '12345', 'metricdata')
    client3 = InfluxDBClient('192.168.4.33', 8086, 'test', '12345', 'Online_Classification')
    rs = client.query("SELECT * from unit1 GROUP BY * ORDER BY DESC LIMIT 1;")
    rs_1 = client.query("SELECT * from station GROUP BY * ORDER BY DESC LIMIT 1;")
    rs2 = client2.query("SELECT UAI, UAH, UFTI, UPPT, URRI, USC, USI, USRI from unit1 GROUP BY * ORDER BY DESC LIMIT 1;")
    data1 = list(rs.get_points())
    data2 = list(rs2.get_points())
    data3 =list(rs_1.get_points())
    rs2_stat = client2.query("SELECT * from station GROUP BY * ORDER BY DESC LIMIT 1;")
    data4 = list(rs2_stat.get_points())

    rs3_stat_loss_index = client3.query("SELECT loss_index from unit1 GROUP BY * ORDER BY DESC LIMIT 1;")
    data_loss_index = list(rs3_stat_loss_index.get_points())

    rs_power_e = client3.query("SELECT power_e from unit1 GROUP BY * ORDER BY DESC LIMIT 1;")
    data_power_e = list(rs_power_e.get_points())

    rs_gas_fuel_flow_y = client3.query("SELECT gas_fuel_flow_y from unit1 GROUP BY * ORDER BY DESC LIMIT 1;")
    data_gas_fuel_flow_y = list(rs_gas_fuel_flow_y.get_points())

    rs_waterwash_status = client3.query("SELECT waterwash_status from unit1 GROUP BY * ORDER BY DESC LIMIT 1;")
    data_waterwash_status = list(rs_waterwash_status.get_points())

    rs_gas_fuel_flow_status = client3.query("SELECT gas_fuel_flow_status from unit1 GROUP BY * ORDER BY DESC LIMIT 1;")
    data_gas_fuel_flow_status = list(rs_gas_fuel_flow_status.get_points())

    rs_waterwash_rate = client3.query("SELECT waterwash_rate from unit1 GROUP BY * ORDER BY DESC LIMIT 1;")
    data_waterwash_rate = list(rs_waterwash_rate.get_points())

    rs_waterwash_time = client3.query("SELECT waterwash_time from unit1 GROUP BY * ORDER BY DESC LIMIT 1;")
    data_waterwash_time = list(rs_waterwash_time.get_points())



    status = dict()
    status['choice'] = 0

    try:
        waterwash_time = round(float(data_waterwash_time[0]['waterwash_time']),2)
    except:
        waterwash_time = "Нет данных"

    try:
        dict_data = data1[0]
        dict_data_ss = data2[0]
        dict_data_stat = data3[0]
        dict_data_ss_stat = data4[0]

        loss_index1 = data_loss_index[0]['loss_index']
        power_e1 = data_power_e[0]['power_e']
        gas_fuel_flow_y1 = data_gas_fuel_flow_y[0]['gas_fuel_flow_y']
        waterwash_status1 = data_waterwash_status[0]['waterwash_status']
        gas_fuel_flow_status1 = data_gas_fuel_flow_status[0]['gas_fuel_flow_status']
        waterwash_rate1 = data_waterwash_rate[0]['waterwash_rate']


    except:

        loss_index = "Нет данных"
        power_e = "Нет данных"
        gas_fuel_flow_y = "Нет данных"
        waterwash_status = "Нет данных"
        gas_fuel_flow_status = "Нет данных"
        waterwash_rate = "Нет данных"



    press_out = round(dict_data_stat['press_out'], 2)






    try:
        loss_index = round(float(loss_index1), 2)
    except:
        loss_index = "Нет данных"
    try:
        power_e = round(float(power_e1), 2)
    except:
        power_e = "Нет данных"

    try:
        gas_fuel_flow_y = round(float(gas_fuel_flow_y1), 2)
    except:
        gas_fuel_flow_y = "Нет данных"

    try:
        waterwash_status = round(float(waterwash_status1), 2)
    except:
        waterwash_status = "Нет данных"

    try:
        gas_fuel_flow_status = round(float(gas_fuel_flow_status1), 2)
    except:
        gas_fuel_flow_status = "Нет данных"

    try:
        waterwash_rate = round(float(waterwash_rate1), 2)
    except:
        waterwash_rate = "Нет данных"

    UAI_CM = dict_data_ss['UAI']





    #
    # try:
    #     UAI_CM = round(dict_data_ss['UAI'], 2)
    # except:
    #     UAI_CM = "Нет данных"
    try:
        UAH_CM = round(dict_data_ss['UAH'], 2)
    except:
        UAH_CM = "Нет данных"

    try:
        URRI_CM = round(dict_data_ss['URRI'], 2)
    except:
        URRI_CM = "Нет данных"

    try:
        SAI_CM = round(dict_data_ss_stat['SAI'], 2)
    except:
        SAI_CM = "Нет данных"

    try:
        SRRI_CM = round(dict_data_ss_stat['SRRI'], 2)
    except:
        SRRI_CM = "Нет данных"

    try:
        URRI_CM = round(dict_data_ss['URRI'], 2)
    except:
        URRI_CM = "Нет данных"

    try:
        SFTI = round(dict_data_ss_stat['SFTI'], 2)
    except:
        SFTI = "Нет данных"





    if message.text == "Станция":
        user_step[cid] = 8
        text_1 = """
        KPI:
        - Готовность станций: *{}%*
        - Производительность станций: *{}%*
        - Надежность станций: *{}%*
        - Количество ГПА в работе: *{}*.
        - Давление на входе станций: *{}МПа*
        - Давление на выходе станций: *{}Мпа*
        """.format(SAI_CM, SFTI, SRRI_CM, dict_data_stat['total_unit_run'], dict_data_stat['press_in'], press_out)
        bot.send_message(message.chat.id, text_1, parse_mode='Markdown', reply_markup=markup_menu_3_1)

    elif message.text == "ГПА 1":
        user_step[cid] = 8

        if dict_data_ss['USI'] == 1:
            status['choice'] = 'Не готов'

        if dict_data_ss['USI'] == 2:
            status['choice'] = 'В резерве'

        if dict_data_ss['USI'] == 3:
            status['choice'] = 'В работе'

        if dict_data_ss['USI'] == 4:
            status['choice'] = 'Магистраль'



        if int(waterwash_status) == 1:
            waterwash_status_string = "Необходимо"
        else:
            waterwash_status_string = "В нормальном состояние"




        text_2 = """
        ГПА1: {}.
        Счетчики:
        - Количество пусков: *{}*
        - Количество часов: *{}ч.*
        - Количество АО: *{}*
        Эффективность:
        Готовность: *{}%*
        Производительность: *{}%*
        Надежность: *{}%*
        Заплан. время до ТО: *{}ч.*
        КПД(относит.): 30%
        Индекс топливного транспорта: *{}%*
        КПД HPC: *{}%*
        Соотношение ExhaustTT к скорости OK:
        Соотношение темп-ры воздуха (TIT):
        Запас по помпажу (TRD): *{}%*
        Индекс потери мощности: *{}%*
        Мощность ГПА по двигателю: *{}кВт*
        Удельный расход топливного газа по турбине: *{}%*
        Статус об необходимости промывки двигателя: *{}*
        Повышенный удельный расход ТГ: *{}*
        Количество часов с последней даты промывки: *{}*
        Скорость приращения удельного расхода по двигателю в следствии деградации ОК: *{}%/день*
        """.format(status['choice'], dict_data['start_count'], dict_data['engine_fired_hours'], dict_data['emrg_stop_cnt'], UAI_CM, dict_data_ss['UFTI'], URRI_CM, UAH_CM,
                   URRI_CM, dict_data['hpc_eta'], dict_data['hpc_turndown'],loss_index, power_e,
                   gas_fuel_flow_y, waterwash_status_string,
                   gas_fuel_flow_status, waterwash_rate, waterwash_time)
        bot.send_message(message.chat.id, text_2, parse_mode='Markdown', reply_markup=markup_menu_3_1)

    elif message.text == "ГПА 2":
        user_step[cid] = 8

        if dict_data_ss['USI'] == 1:
            status['choice'] = 'Не готов'

        if dict_data_ss['USI'] == 2:
            status['choice'] = 'В резерве'

        if dict_data_ss['USI'] == 3:
            status['choice'] = 'В работе'

        if dict_data_ss['USI'] == 4:
            status['choice'] = 'Магистраль'



        if int(waterwash_status) == 1:
            waterwash_status_string = "Необходимо"
        else:
            waterwash_status_string = "В нормальном состояние"

        text_3 = """
        ГПА2: {}.
        Счетчики:
        - Количество пусков: *{}*
        - Количество часов: *{}ч.*
        - Количество АО: *{}*
        Эффективность:
        Готовность: *{}%*
        Производительность: *{}%*
        Надежность: *{}%*
        Заплан. время до ТО: *{}ч.*
        КПД(относит.): 30%
        Индекс топливного транспорта: *{}%*
        КПД HPC: *{}%*
        Соотношение ExhaustTT к скорости OK:
        Соотношение темп-ры воздуха (TIT):
        Запас по помпажу (TRD): *{}%*
        Индекс потери мощности: *{}%*
        Мощность ГПА по двигателю: *{}кВт*
        Удельный расход топливного газа по турбине: *{}%*
        Статус об необходимости промывки двигателя: *{}*
        Повышенный удельный расход ТГ: *{}*
        Количество часов с последней даты промывки: *{}*
        Скорость приращения удельного расхода по двигателю в следствии деградации ОК: *{}%/день*
        """.format(status['choice'], dict_data['start_count'], dict_data['engine_fired_hours'], dict_data['emrg_stop_cnt'], UAI_CM, dict_data_ss['UFTI'], URRI_CM, UAH_CM,
                   URRI_CM, dict_data['hpc_eta'], dict_data['hpc_turndown'],loss_index,power_e,
                   gas_fuel_flow_y, waterwash_status_string,
                   gas_fuel_flow_status, waterwash_rate, waterwash_time)
        bot.send_message(message.chat.id, text_3, parse_mode='Markdown', reply_markup=markup_menu_3_1)

    elif message.text == "ГПА 3":
        user_step[cid] = 8

        if dict_data_ss['USI'] == 1:
            status['choice'] = 'Не готов'

        if dict_data_ss['USI'] == 2:
            status['choice'] = 'В резерве'

        if dict_data_ss['USI'] == 3:
            status['choice'] = 'В работе'

        if dict_data_ss['USI'] == 4:
            status['choice'] = 'Магистраль'


        if int(waterwash_status) == 1:
            waterwash_status_string = "Необходимо"
        else:
            waterwash_status_string = "В нормальном состояние"

        text_4 = """
        ГПА3: {}.
        Счетчики:
        - Количество пусков: *{}*
        - Количество часов: *{}ч.*
        - Количество АО: *{}*
        Эффективность:
        Готовность: *{}%*
        Производительность: *{}%*
        Надежность: *{}%*
        Заплан. время до ТО: *{}ч.*
        КПД(относит.): 30%
        Индекс топливного транспорта: *{}%*
        КПД HPC: *{}%*
        Соотношение ExhaustTT к скорости OK:
        Соотношение темп-ры воздуха (TIT):
        Запас по помпажу (TRD): *{}%*
        Индекс потери мощности: *{}%*
        Мощность ГПА по двигателю: *{}кВт*
        Удельный расход топливного газа по турбине: *{}%*
        Статус об необходимости промывки двигателя: *{}*
        Повышенный удельный расход ТГ: *{}*
        Количество часов с последней даты промывки: *{}*
        Скорость приращения удельного расхода по двигателю в следствии деградации ОК: *{}%/день*
        """.format(status['choice'], dict_data['start_count'], dict_data['engine_fired_hours'], dict_data['emrg_stop_cnt'], UAI_CM, dict_data_ss['UFTI'], URRI_CM, UAH_CM,
                   URRI_CM, dict_data['hpc_eta'], dict_data['hpc_turndown'],loss_index,power_e,
                   gas_fuel_flow_y, waterwash_status_string,
                   gas_fuel_flow_status, waterwash_rate, waterwash_time)
        bot.send_message(message.chat.id, text_4, parse_mode='Markdown', reply_markup=markup_menu_3_1)

    elif message.text == "Назад":
        user_step[cid] = 2
        bot.send_message(message.chat.id, "Выберите КС", reply_markup=markup_menu_2)

    elif message.text == "В начало":
        user_step[cid] = 1
        bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup_menu_1)


## Menu when user choose statistics/stancia or gpa1-3
@bot.message_handler(func=lambda message: get_user_step(message.chat.id) == 8)
def main_menu(message):
    cid = message.chat.id

    if message.text == "Назад":
        user_step[cid] = 3
        bot.send_message(message.chat.id, "Выберите объект", reply_markup=markup_menu_3)

    elif message.text == "В начало":
        user_step[cid] = 1
        bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup_menu_1)




## Menu when user choose otchet
@bot.message_handler(func=lambda message: get_user_step(message.chat.id) == 4)
def main_menu(message):
    cid = message.chat.id
    if message.text == "КС Караозек":
        user_step[cid] = 5
        bot.send_message(message.chat.id, "Выберите диапазон времени", reply_markup=markup_menu_5)
    elif message.text == "Назад":
        user_step[cid] = 1
        bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup_menu_1)





## Menu when user choose otchet/ ks karaozek
@bot.message_handler(func=lambda message: get_user_step(message.chat.id) == 5)
def main_menu(message):
    cid = message.chat.id
    client = InfluxDBClient(host='192.168.4.33', port=8086, username='test', password='12345', database = 'Labview')
    client2 = InfluxDBClient(host='192.168.4.33', port=8086, username='test', password='12345', database= 'metricdata')
    client3 = InfluxDBClient(host='192.168.4.33', port=8086, username='test', password='12345')

    if message.text == "Час":
        user_step[cid] = 9
#########################
        rs = client3.query("SELECT difference(last(gas_common)), difference(last(gas_losses)), difference(last(fuel_gas_common)), difference(last(power_com)) FROM Labview.autogen.station WHERE time > now() - 1h GROUP BY time(1h);")
        dta = list(rs.get_points())
        try:
            gas_common = round(dta[0]['difference'], 2)
            gas_losses = round(dta[0]['difference_1'], 2)
            fuel_gas_common = round(dta[0]['difference_2'], 2)
            power_com = round(dta[0]['difference_3'], 2)
        except:
            gas_common = 'Нет данных'
            gas_losses = 'Нет данных'
            fuel_gas_common = 'Нет данных'
            power_com = 'Нет данных'


        rs1 = client3.query(
            "SELECT difference(last(gas_total)), difference(last(fuel_gas_total)), difference(last(gas_spent_total)) FROM Labview.autogen.unit1 WHERE time > now() - 1h GROUP BY time(1h);")
        dta1 = list(rs1.get_points())
        try:
            gas_total_u1 = round(dta1[0]['difference'],2)
            fuel_gas_total_u1 = round(dta1[0]['difference_1'],2)
            gas_spent_total_u1 = round(dta1[0]['difference_2'],2)
        except:
            gas_total_u1 = 'Нет данных'
            fuel_gas_total_u1 = 'Нет данных'
            gas_spent_total_u1 = 'Нет данных'


        rs2 = client3.query(
            "SELECT difference(last(gas_total)), difference(last(fuel_gas_total)), difference(last(gas_spent_total)) FROM Labview.autogen.unit2 WHERE time > now() - 1h GROUP BY time(1h);")
        dta2 = list(rs2.get_points())
        try:
            gas_total_u2 = round(dta2[0]['difference'],2)
            fuel_gas_total_u2 = round(dta2[0]['difference_1'],2)
            gas_spent_total_u2 = round(dta2[0]['difference_2'],2)
        except:
            gas_total_u2 = 'Нет данных'
            fuel_gas_total_u2 = 'Нет данных'
            gas_spent_total_u2 = 'Нет данных'

        rs3 = client3.query(
            "SELECT difference(last(gas_total)), difference(last(fuel_gas_total)), difference(last(gas_spent_total)) FROM Labview.autogen.unit3 WHERE time > now() - 1h GROUP BY time(1h);")
        dta3 = list(rs3.get_points())
        try:
            gas_total_u3 = round(dta3[0]['difference'], 2)
            fuel_gas_total_u3 = round(dta3[0]['difference_1'], 2)
            gas_spent_total_u3 = round(dta3[0]['difference_2'], 2)
        except:
            gas_total_u3 = 'Нет данных'
            fuel_gas_total_u3 = 'Нет данных'
            gas_spent_total_u3 = 'Нет данных'

        rs4 = client3.query(
            "SELECT difference(last(SSD)) FROM metricdata.autogen.station WHERE time > now() - 1h GROUP BY time(1h);")
        dta4 = list(rs4.get_points())
        try:
            ssd = round(dta4[0]['difference'], 2)
        except:
            ssd = 'Нет данных'




###############################

        text_1 = """
        Станция:
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*
        - Потребление электроэнерг.: *{}* MW
        - Аварийные остановы: *{}*

        ГПА 1
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*

        ГПА 2
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*

        ГПА 3
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*
        """.format(gas_common, fuel_gas_common, gas_losses, power_com, ssd,  gas_total_u1, fuel_gas_total_u1, gas_spent_total_u1, gas_total_u2, fuel_gas_total_u2, gas_spent_total_u2,
                   gas_total_u3, fuel_gas_total_u3, gas_spent_total_u3)
        bot.send_message(message.chat.id, text_1, parse_mode='Markdown', reply_markup=markup_menu_5_1)


    elif message.text == "Сутки":
        user_step[cid] = 9
####################################3333
        rs = client3.query(
            "SELECT difference(last(gas_common)), difference(last(gas_losses)), difference(last(fuel_gas_common)), difference(last(power_com)) FROM Labview.autogen.station WHERE time > now() - 1d GROUP BY time(1d);")
        dta = list(rs.get_points())
        try:
            gas_common = round(dta[0]['difference'], 2)
            gas_losses = round(dta[0]['difference_1'], 2)
            fuel_gas_common = round(dta[0]['difference_2'], 2)
            power_com = round(dta[0]['difference_3'], 2)
        except:
            gas_common = 'Нет данных'
            gas_losses = 'Нет данных'
            fuel_gas_common = 'Нет данных'
            power_com = 'Нет данных'

        rs1 = client3.query(
            "SELECT difference(last(gas_total)), difference(last(fuel_gas_total)), difference(last(gas_spent_total)) FROM Labview.autogen.unit1 WHERE time > now() - 1d GROUP BY time(1d);")
        dta1 = list(rs1.get_points())
        try:
            gas_total_u1 = round(dta1[0]['difference'], 2)
            fuel_gas_total_u1 = round(dta1[0]['difference_1'], 2)
            gas_spent_total_u1 = round(dta1[0]['difference_2'], 2)
        except:
            gas_total_u1 = 'Нет данных'
            fuel_gas_total_u1 = 'Нет данных'
            gas_spent_total_u1 = 'Нет данных'

        rs2 = client3.query(
            "SELECT difference(last(gas_total)), difference(last(fuel_gas_total)), difference(last(gas_spent_total)) FROM Labview.autogen.unit2 WHERE time > now() - 1d GROUP BY time(1d);")
        dta2 = list(rs2.get_points())
        try:
            gas_total_u2 = round(dta2[0]['difference'], 2)
            fuel_gas_total_u2 = round(dta2[0]['difference_1'], 2)
            gas_spent_total_u2 = round(dta2[0]['difference_2'], 2)
        except:
            gas_total_u2 = 'Нет данных'
            fuel_gas_total_u2 = 'Нет данных'
            gas_spent_total_u2 = 'Нет данных'

        rs3 = client3.query(
            "SELECT difference(last(gas_total)), difference(last(fuel_gas_total)), difference(last(gas_spent_total)) FROM Labview.autogen.unit3 WHERE time > now() - 1d GROUP BY time(1d);")
        dta3 = list(rs3.get_points())
        try:
            gas_total_u3 = round(dta3[0]['difference'], 2)
            fuel_gas_total_u3 = round(dta3[0]['difference_1'], 2)
            gas_spent_total_u3 = round(dta3[0]['difference_2'], 2)
        except:
            gas_total_u3 = 'Нет данных'
            fuel_gas_total_u3 = 'Нет данных'
            gas_spent_total_u3 = 'Нет данных'

        rs4 = client3.query(
            "SELECT difference(last(SSD)) FROM metricdata.autogen.station WHERE time > now() - 1d GROUP BY time(1d);")
        dta4 = list(rs4.get_points())
        try:
            ssd = round(dta4[0]['difference'], 2)
        except:
            ssd = 'Нет данных'
##############################################
        text_3 = """
        Станция:
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*
        - Потребление электроэнерг.: *{}* MW
        - Аварийные остановы: *{}*

        ГПА 1
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*

        ГПА 2
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*

        ГПА 3
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*
        """.format(gas_common, fuel_gas_common, gas_losses, power_com, ssd, gas_total_u1, fuel_gas_total_u1,
                   gas_spent_total_u1, gas_total_u2, fuel_gas_total_u2, gas_spent_total_u2,
                   gas_total_u3, fuel_gas_total_u3, gas_spent_total_u3)
        bot.send_message(message.chat.id, text_3, parse_mode='Markdown', reply_markup=markup_menu_5_1)



    elif message.text == "Неделя":
        user_step[cid] = 9
        rs5 = client.query("SELECT * from station WHERE TIME > now() - {0}d;".format(datetime.date.today().strftime("%w")))
        data5 = list(rs5.get_points())
        rs6 = client.query("SELECT * from station GROUP BY * ORDER BY DESC LIMIT 1;")
        data6 = list(rs6.get_points())

        rs7 = client.query("SELECT * from unit1 WHERE TIME > now()- {0}d;".format(datetime.date.today().strftime("%w")))
        data7 = list(rs7.get_points())
        rs8 = client.query("SELECT * from unit1 GROUP BY * ORDER BY DESC LIMIT 1;")
        data8 = list(rs8.get_points())

        rs7_2 = client.query("SELECT * from unit2 WHERE TIME > now() - {0}d;".format(datetime.date.today().strftime("%w")))
        data7_2 = list(rs7_2.get_points())
        rs8_2 = client.query("SELECT * from unit2 GROUP BY * ORDER BY DESC LIMIT 1;")
        data8_2 = list(rs8_2.get_points())
        rs7_3 = client.query("SELECT * from unit3 WHERE TIME > now() - {0}d;".format(datetime.date.today().strftime("%w")))
        data7_3 = list(rs7_3.get_points())
        rs8_3 = client.query("SELECT * from unit3 GROUP BY * ORDER BY DESC LIMIT 1;")
        data8_3 = list(rs8_3.get_points())

        rs_ss_1 = client2.query("SELECT * from station WHERE TIME > now() - {0}d;".format(datetime.date.today().strftime("%w")))
        data9 = list(rs_ss_1.get_points())
        rs_ss_2 = client2.query("SELECT * from station GROUP BY * ORDER BY DESC LIMIT 1;")
        data10 = list(rs_ss_2.get_points())

        try:
            dict_data_day_before = data5[0]
            dict_data_current = data6[0]
        except:
            pass


        try:
            dict_data_day_before_stat_ss = data9[0]
            dict_data_current_stat_ss = data10[0]
        except:
            pass

        try:
            dict_data_day_before_u1 = data7[0]
            dict_data_current_u1 = data8[0]
        except:
            pass

        try:
            dict_data_day_before_u2 = data7_2[0]
            dict_data_current_u2 = data8_2[0]
        except:
            pass

        try:
            dict_data_day_before_u3 = data7_3[0]
            dict_data_current_u3 = data8_3[0]
        except:
            pass


        try:
            gas_common = round(dict_data_current['gas_common'] - dict_data_day_before['gas_common'], 2)
        except:
            gas_common = 'Нет данных'

        try:
            gas_losses = round(dict_data_current['gas_losses'] - dict_data_day_before['gas_losses'], 2)
        except:
            gas_losses = 'Нет данных'

        try:
            fuel_gas_common = round(dict_data_current['fuel_gas_common'] - dict_data_day_before['fuel_gas_common'], 2)
        except:
            fuel_gas_common = 'Нет данных'

        try:
            power_com = round(dict_data_current['power_com'] - dict_data_day_before['power_com'], 2)
        except:
            power_com = 'Нет данных'



        try:
            gas_total_u1 = round(dict_data_current_u1['gas_total'] - dict_data_day_before_u1['gas_total'], 2)
            fuel_gas_total_u1 = round(
                dict_data_current_u1['fuel_gas_total'] - dict_data_day_before_u1['fuel_gas_total'], 2)
            gas_spent_total_u1 = round(
                dict_data_current_u1['gas_spent_total'] - dict_data_day_before_u1['gas_spent_total'], 2)
        except:
            gas_total_u1 = 'Нет данных'
            fuel_gas_total_u1 = 'Нет данных'
            gas_spent_total_u1 = 'Нет данных'

        try:
            gas_total_u2 = round(dict_data_current_u2['gas_total'] - dict_data_day_before_u2['gas_total'], 2)
            fuel_gas_total_u2 = round(
                dict_data_current_u2['fuel_gas_total'] - dict_data_day_before_u2['fuel_gas_total'], 2)
            gas_spent_total_u2 = round(
                dict_data_current_u2['gas_spent_total'] - dict_data_day_before_u2['gas_spent_total'], 2)
        except:
            gas_total_u2 = 'Нет данных'
            fuel_gas_total_u2 = 'Нет данных'
            gas_spent_total_u2 = 'Нет данных'

        try:
            gas_total_u3 = round(dict_data_current_u3['gas_total'] - dict_data_day_before_u3['gas_total'], 2)
            fuel_gas_total_u3 = round(
                dict_data_current_u3['fuel_gas_total'] - dict_data_day_before_u3['fuel_gas_total'], 2)
            gas_spent_total_u3 = round(
                dict_data_current_u3['gas_spent_total'] - dict_data_day_before_u3['gas_spent_total'], 2)
        except:
            gas_total_u3 = 'Нет данных'
            fuel_gas_total_u3 = 'Нет данных'
            gas_spent_total_u3 = 'Нет данных'

        try:
            ssd = dict_data_current_stat_ss['SSD'] - dict_data_day_before_stat_ss['SSD']
        except:
            ssd = 'Нет данных'

        text_4 = """
        Станция:
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*
        - Потребление электроэнерг.: *{}* MW
        - Аварийные остановы: *{}*

        ГПА 1
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*

        ГПА 2
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*

        ГПА 3
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*
        """.format(gas_common, fuel_gas_common, gas_losses, power_com, ssd, gas_total_u1, fuel_gas_total_u1,
                   gas_spent_total_u1, gas_total_u2, fuel_gas_total_u2, gas_spent_total_u2,
                   gas_total_u3, fuel_gas_total_u3, gas_spent_total_u3)
        bot.send_message(message.chat.id, text_4, parse_mode='Markdown', reply_markup=markup_menu_5_1)


    elif message.text == "Месяц":
        user_step[cid] = 9
        rs5 = client.query("SELECT * from station WHERE TIME > now() - {0}d;".format(datetime.date.today().strftime("%d")))
        data5 = list(rs5.get_points())
        rs6 = client.query("SELECT * from station GROUP BY * ORDER BY DESC LIMIT 1;")
        data6 = list(rs6.get_points())

        rs7 = client.query("SELECT * from unit1 WHERE TIME > now() - {0}d;".format(datetime.date.today().strftime("%d")))
        data7 = list(rs7.get_points())
        rs8 = client.query("SELECT * from unit1 GROUP BY * ORDER BY DESC LIMIT 1;")
        data8 = list(rs8.get_points())
        rs7_2 = client.query("SELECT * from unit2 WHERE TIME > now() - {0}d;".format(datetime.date.today().strftime("%d")))
        data7_2 = list(rs7_2.get_points())
        rs8_2 = client.query("SELECT * from unit2 GROUP BY * ORDER BY DESC LIMIT 1;")
        data8_2 = list(rs8_2.get_points())
        rs7_3 = client.query("SELECT * from unit3 WHERE TIME > now() - {0}d;".format(datetime.date.today().strftime("%d")))
        data7_3 = list(rs7_3.get_points())
        rs8_3 = client.query("SELECT * from unit3 GROUP BY * ORDER BY DESC LIMIT 1;")
        data8_3 = list(rs8_3.get_points())

        rs_ss_1 = client2.query("SELECT * from station WHERE TIME > now() - {0}d;".format(datetime.date.today().strftime("%d")))
        data9 = list(rs_ss_1.get_points())
        rs_ss_2 = client2.query("SELECT * from station GROUP BY * ORDER BY DESC LIMIT 1;")
        data10 = list(rs_ss_2.get_points())

        try:
            dict_data_day_before = data5[0]
            dict_data_current = data6[0]
        except:
            pass

        try:
            dict_data_day_before_stat_ss = data9[0]
            dict_data_current_stat_ss = data10[0]
        except:
            pass

        try:
            dict_data_day_before_u1 = data7[0]
            dict_data_current_u1 = data8[0]
        except:
            pass

        try:
            dict_data_day_before_u2 = data7_2[0]
            dict_data_current_u2 = data8_2[0]
        except:
            pass

        try:
            dict_data_day_before_u3 = data7_3[0]
            dict_data_current_u3 = data8_3[0]
        except:
            pass

        ## Catch exception when on of get data as none
        try:
            gas_common = round(dict_data_current['gas_common'] - dict_data_day_before['gas_common'], 2)
        except:
            gas_common = 'Нет данных'

        try:
            gas_losses = round(dict_data_current['gas_losses'] - dict_data_day_before['gas_losses'], 2)
        except:
            gas_losses = 'Нет данных'

        try:
            fuel_gas_common = round(dict_data_current['fuel_gas_common'] - dict_data_day_before['fuel_gas_common'],
                                    2)
        except:
            fuel_gas_common = 'Нет данных'

        try:
            power_com = round(dict_data_current['power_com'] - dict_data_day_before['power_com'], 2)
        except:
            power_com = 'Нет данных'

        try:
            gas_total_u1 = round(dict_data_current_u1['gas_total'] - dict_data_day_before_u1['gas_total'], 2)
            fuel_gas_total_u1 = round(
                dict_data_current_u1['fuel_gas_total'] - dict_data_day_before_u1['fuel_gas_total'], 2)
            gas_spent_total_u1 = round(
                dict_data_current_u1['gas_spent_total'] - dict_data_day_before_u1['gas_spent_total'], 2)
        except:
            gas_total_u1 = 'Нет данных'
            fuel_gas_total_u1 = 'Нет данных'
            gas_spent_total_u1 = 'Нет данных'

        try:
            gas_total_u2 = round(dict_data_current_u2['gas_total'] - dict_data_day_before_u2['gas_total'], 2)
            fuel_gas_total_u2 = round(
                dict_data_current_u2['fuel_gas_total'] - dict_data_day_before_u2['fuel_gas_total'], 2)
            gas_spent_total_u2 = round(
                dict_data_current_u2['gas_spent_total'] - dict_data_day_before_u2['gas_spent_total'], 2)
        except:
            gas_total_u2 = 'Нет данных'
            fuel_gas_total_u2 = 'Нет данных'
            gas_spent_total_u2 = 'Нет данных'

        try:
            gas_total_u3 = round(dict_data_current_u3['gas_total'] - dict_data_day_before_u3['gas_total'], 2)
            fuel_gas_total_u3 = round(
                dict_data_current_u3['fuel_gas_total'] - dict_data_day_before_u3['fuel_gas_total'], 2)
            gas_spent_total_u3 = round(
                dict_data_current_u3['gas_spent_total'] - dict_data_day_before_u3['gas_spent_total'], 2)
        except:
            gas_total_u3 = 'Нет данных'
            fuel_gas_total_u3 = 'Нет данных'
            gas_spent_total_u3 = 'Нет данных'

        try:
            ssd = dict_data_current_stat_ss['SSD'] - dict_data_day_before_stat_ss['SSD']
        except:
            ssd = 'Нет данных'



        text_5 = """
        Станция:
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*
        - Потребление электроэнерг.: *{}* MW
        - Аварийные остановы: *{}*

        ГПА 1
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*

        ГПА 2
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*

        ГПА 3
        - Транспорт газа: *{} млн м3*
        - Потребление газа: *{} м3*
        - Потери техн. газа: *{} м3*
        """.format(gas_common, fuel_gas_common, gas_losses, power_com, ssd, gas_total_u1, fuel_gas_total_u1,
                   gas_spent_total_u1, gas_total_u2, fuel_gas_total_u2, gas_spent_total_u2,
                   gas_total_u3, fuel_gas_total_u3, gas_spent_total_u3)
        bot.send_message(message.chat.id, text_5, parse_mode='Markdown', reply_markup=markup_menu_5_1)

    elif message.text == "Назад":
        user_step[cid] = 4
        bot.send_message(message.chat.id, "Выберите КС", reply_markup=markup_menu_4)

    elif message.text == "В начало":
        user_step[cid] = 1
        bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup_menu_1)



## Menu when user choose otchet/ ks karaozek / den - god
@bot.message_handler(func=lambda message: get_user_step(message.chat.id) == 9)
def main_menu(message):
    cid = message.chat.id

    if message.text == "Назад":
        user_step[cid] = 5
        bot.send_message(message.chat.id, "Выберите диапазон времени", reply_markup=markup_menu_5)

    elif message.text == "В начало":
        user_step[cid] = 1
        bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup_menu_1)




## Menu when user choose anomalii
@bot.message_handler(func=lambda message: get_user_step(message.chat.id) == 6)
def main_menu(message):
    cid = message.chat.id
    last_name = message.chat.last_name
    first_name = message.chat.first_name
    if message.text == "Да":
        user_step[cid] = 7
        text_1 = """
                Выберите КС
                """
        bot.send_message(message.chat.id, text_1, reply_markup=markup_menu_7)

    elif message.text == "Отказаться от всех подписок":

        header = {'Content-Type': 'application/json',

                  'Accept': 'application/json'}

        data1 = {}

        data1['User'] = cid

        data1['Status'] = 0

        data2 = json.dumps(data1)
        print(str(first_name) + " " + str(last_name) + " unsubscribed from anomaly")
        try:
            response = requests.post(
            url='http://localhost:5000/users',
            data=json.dumps(data2), headers=header)

        except:
            print('Print host can not connect')
            pass

        user_step[cid] = 1
        bot.send_message(message.chat.id, "Вы отписались!", reply_markup=markup_menu_1)


    elif message.text == "Нет":
            user_step[cid] = 1
            bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup_menu_1)


## Menu when user choose anomalii/ da
@bot.message_handler(func=lambda message: get_user_step(message.chat.id) == 7)
def main_menu(message):
    cid = message.chat.id
    last_name = message.chat.last_name
    first_name = message.chat.first_name
    if message.text == "КС Караозек":

        header = {'Content-Type': 'application/json',
                  'Accept': 'application/json'}
        data1 = dict()
        data1['User'] = cid
        data1['Status'] = 1
        data2 = json.dumps(data1)
        print(str(first_name)+" "+str(last_name)+" subscribed to anomaly")
        try:
            response = requests.post(
            url='http://localhost:5000/users',
            data=json.dumps(data2), headers=header)
        except:
            print('Print host can not connect')
            pass
        user_step[cid] = 10
        text_1 = """
                Вы оформили подписку и будете получать уведомление по аномалиям КС Караозек.
                """
        bot.send_message(message.chat.id, text_1, reply_markup=markup_menu_7_1)

    elif message.text == "Назад":
        user_step[cid] = 6
        bot.send_message(message.chat.id, "Желаете ли Вы подписаться на уведомление аномалий?", reply_markup=markup_menu_6)

    elif message.text == "В начало":
        user_step[cid] = 1
        bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup_menu_1)



## Menu when user choose anomalii/ da /
@bot.message_handler(func=lambda message: get_user_step(message.chat.id) == 10)
def main_menu(message):
    cid = message.chat.id

    if message.text == "Назад":
        user_step[cid] = 6
        bot.send_message(message.chat.id, "Желаете ли Вы подписаться на уведомление аномалий?", reply_markup=markup_menu_6)

    elif message.text == "В начало":
        user_step[cid] = 1
        bot.send_message(message.chat.id, "Выберите действие", reply_markup=markup_menu_1)



if __name__ == "__main__":
    while True:
        try:
            bot.polling(none_stop=True, timeout = 300)
        except Exception as e:
            print(e)
            time.sleep(5)
# bot.polling(none_stop=True, interval = 0, timeout = 180)





