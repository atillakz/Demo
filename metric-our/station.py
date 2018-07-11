#!/home/user/anaconda3/bin/python
from stationclass import *

user = "userzsse"

password = "Zeinet8sse"

host = "80.241.40.230"

database = "zsse"

Unit1_query = ("SELECT id,USI,USC,UPPT,UFTI,UAI,USRI,URRI,UAH FROM zsse.Unit1_metrics ORDER BY id DESC LIMIT 1")

Unit2_query = ("SELECT id,USI,USC,UPPT,UFTI,UAI,USRI,URRI,UAH FROM zsse.Unit2_metrics ORDER BY id DESC LIMIT 1")

Unit3_query = ("SELECT id,USI,USC,UPPT,UFTI,UAI,USRI,URRI,UAH FROM zsse.Unit3_metrics ORDER BY id DESC LIMIT 1")

Unit4_query = ("SELECT id,fuel_gas_common,gas_losses,gas_common FROM zsse.Station ORDER BY id DESC LIMIT 1")


id_to_check_1 = dict()
id_to_check_2 = dict()
id_to_check_3 = dict()
id_to_check_4 = dict()

id_to_check_1['check'] = 0
id_to_check_2['check'] = 0
id_to_check_3['check'] = 0
id_to_check_4['check'] = 0





query_insert = (
"INSERT INTO Station_metrics (dateandtime,SAI,SFTI,SSRI,SRRI,SRP,STUP,SGTC,SPPI,TURT,CGL,SBSD,SSD) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")

starttime=time.time()

while True:


    estimator = Compute_parameters_for_station(user, password, host, database, Unit1_query, Unit2_query, Unit3_query,Unit4_query)

    estimator.connect_to_sql()

    print("STATION is CONNECTED.")

    id_1 = estimator.prepare_data_one()

    id_2 = estimator.prepare_data_two()

    id_3 = estimator.prepare_data_three()

    id_4 = estimator.prepare_data_four()

    if id_to_check_1['check'] != id_1 and id_to_check_2['check'] !=id_2 and id_to_check_3['check'] != id_3 and id_to_check_4['check'] != id_4:

        dateandtime = estimator.date()
        SRP = estimator.all()
        STUP = estimator.all()
        SGTC = estimator.all()
        SPPI = estimator.all()
        TURT = estimator.all()
        CGL = estimator.all()
        SBSD = estimator.all()
        SSD = estimator.all()
        SAI = estimator.sai()
        SFTI = estimator.sfti()
        SSRI = estimator.ssri()
        SRRI = estimator.srri()

        try:

             #print(dateandtime,SAI,SFTI,SSRI,SRRI,SRP,STUP,SGTC,SPPI,TURT,CGL,SBSD,SSD)

             cnx_insert = mysql.connector.connect(user=user, password=password, host=host,
                                             database=database)
             cursor_insert = cnx_insert.cursor()

             cursor_insert.execute(query_insert,(dateandtime,SAI,SFTI,SSRI,SRRI,SRP,STUP,SGTC,SPPI,TURT,CGL,SBSD,SSD))

             cnx_insert.commit()

             cnx_insert.close()

        except:

            print("There is NO DATABASE to INSERT")

    id_to_check_1['check'] = id_1
    id_to_check_2['check'] = id_2
    id_to_check_3['check'] = id_3
    id_to_check_4['check'] = id_4



    time.sleep(35.0 - ((time.time() - starttime) % 35.0))



