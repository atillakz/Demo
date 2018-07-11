import mysql.connector
from mysql.connector import errorcode
from datetime import datetime, timedelta
import time

#source ~/venvs/flaskproj/bin/activate

class Compute_parameters_for_station():

    def __init__(self, user, password, host, database, query_for_unit_1, query_for_unit_2, query_for_unit_3,query_for_unit_4):

        self.user = user

        self.password = password

        self.host = host

        self.database = database

        self.query_for_unit_1 = query_for_unit_1

        self.query_for_unit_2 = query_for_unit_2

        self.query_for_unit_3 = query_for_unit_3

        self.query_for_unit_4 = query_for_unit_4

    def connect_to_sql(self):
        try:
            self.cnx_read_one = mysql.connector.connect(user=self.user, password=self.password, host=self.host,
                                                    database=self.database)
            self.cnx_read_two = mysql.connector.connect(user=self.user, password=self.password, host=self.host,
                                                        database=self.database)
            self.cnx_read_three = mysql.connector.connect(user=self.user, password=self.password, host=self.host,
                                                        database=self.database)

            self.cnx_read_four = mysql.connector.connect(user=self.user, password=self.password, host=self.host,
                                                          database=self.database)


        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            #print("Connected")

            self.cursor_read_unit_one = self.cnx_read_one.cursor(dictionary=True)

            self.cursor_read_unit_two = self.cnx_read_two.cursor(dictionary=True)

            self.cursor_read_unit_three = self.cnx_read_three.cursor(dictionary=True)

            self.cursor_read_unit_four = self.cnx_read_four.cursor(dictionary=True)


            self.cursor_read_unit_one.execute(self.query_for_unit_1)

            self.cursor_read_unit_two.execute(self.query_for_unit_2)

            self.cursor_read_unit_three.execute(self.query_for_unit_3)

            self.cursor_read_unit_four.execute(self.query_for_unit_4)


    def prepare_data_one(self):

        for self.row_one in self.cursor_read_unit_one:
            self.id_one = self.row_one['id']
            self.Unit1_UAI = self.row_one['UAI']
            self.Unit1_UFTI = self.row_one['UFTI']
            self.Unit1_USRI = self.row_one['USRI']
            self.Unit1_URRI = self.row_one['URRI']

        return self.id_one

    def prepare_data_two(self):

        for self.row_two in self.cursor_read_unit_two:
            self.id_two = self.row_two['id']
            self.Unit2_UAI = self.row_two['UAI']
            self.Unit2_UFTI = self.row_two['UFTI']
            self.Unit2_USRI = self.row_two['USRI']
            self.Unit2_URRI = self.row_two['URRI']

        return self.id_two

    def prepare_data_three(self):

        for self.row_three in self.cursor_read_unit_three:
          self.id_three = self.row_three['id']
          self.Unit3_UAI = self.row_three['UAI']
          self.Unit3_UFTI = self.row_three['UFTI']
          self.Unit3_USRI = self.row_three['USRI']
          self.Unit3_URRI = self.row_three['URRI']

        return self.id_three

    def prepare_data_four(self):
        for self.row_four in self.cursor_read_unit_four:
            self.id_four = self.row_four['id']
            self.fuel_gas_common = self.row_four['fuel_gas_common']
            self.gas_losses = self.row_four['gas_losses']
            self.gas_common = self.row_four['gas_common']

        return self.id_four

    def date(self):

        dateandtime = datetime.now()

        return dateandtime

    def sai(self):

        return (self.Unit1_UAI + self.Unit2_UAI + self.Unit3_UAI) / 3

    def sfti(self):

        return (self.fuel_gas_common + self.gas_losses)/self.gas_common

    def ssri(self):

        return (self.Unit1_USRI + self.Unit2_USRI + self.Unit3_USRI) / 3

    def srri(self):

        return (self.Unit1_URRI + self.Unit2_URRI + self.Unit3_URRI) / 3

    def all(self):

        return 0























