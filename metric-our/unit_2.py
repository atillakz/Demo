#!/home/user/anaconda3/bin/python

from unitclass import *

user = "userzsse"

password = "Zeinet8sse"

host = "80.241.40.230"

database = "zsse"

query_for_agp = (
    "SELECT id,unit_notready, unit_stdby,unit_run,unit_loaded, emerg_stop_blow_cnt,emrg_stop_cnt, gas_fuel_flow,hpc_flow_act,ufoh, unit_attempts, start_count, dateandtime FROM zsse.Unit2_Labview ORDER BY id DESC LIMIT 1")

starttime = time.time()

id_to_check = dict()

id_to_check['check'] = 0

query_insert_for_agp_one = (
    "INSERT INTO Unit2_metrics (dateandtime,USI,USC,UPPT,UFTI,UAI,USRI,URRI,UAH) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)")

while True:

    estimator = Compute_parameters_for_unit(user, password, host, database, query_for_agp)

    estimator.connect_to_sql()

    print("Unit 2 is CONNECTED.")


    pid = estimator.prepare_data()

    id = pid[0]

    dateandtime = pid[1]

    if id_to_check['check'] != id:

        USI = estimator.usi()

        USC = estimator.usc()

        UPPT = estimator.uppt()

        UFTI = estimator.ufti()

        UAI = estimator.uai()

        USRI = estimator.usri()

        URRI = estimator.urri()

        UAH = estimator.uah()

        try:

            cnx_insert = mysql.connector.connect(user=user, password=password, host=host,
                                                 database=database)

            cursor_insert = cnx_insert.cursor()

            cursor_insert.execute(query_insert_for_agp_one, (dateandtime, USI, USC, UPPT, UFTI, UAI, USRI, URRI, UAH))

            cnx_insert.commit()

            cnx_insert.close()

        except:

            print("UNIT_2 : There is No DATABASE to INSERT ")

    else:

        USI = estimator.nan()

        USC = estimator.nan()

        UPPT = estimator.nan()

        UFTI = estimator.nan()

        UAI = estimator.nan()

        USRI = estimator.nan()

        URRI = estimator.nan()

        UAH = estimator.nan()

    id_to_check['check'] = id
    time.sleep(10.0 - ((time.time() - starttime) % 10.0))







