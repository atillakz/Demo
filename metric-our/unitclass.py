#!/home/user/anaconda3/bin/python

import mysql.connector
from mysql.connector import errorcode
from datetime import datetime, timedelta
import pandas as pd
from influxdb import DataFrameClient

import time


class Compute_parameters_for_unit():

    def __init__(self, user, password, host, database, query_for_agp):

        self.user = user

        self.password = password

        self.host = host

        self.database = database

        self.query_for_agp = query_for_agp

    def connect_to_sql(self):
        try:
            self.cnx_read = mysql.connector.connect(user=self.user, password=self.password, host=self.host,
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

            self.cursor_read = self.cnx_read.cursor(dictionary=True)

            self.cursor_read.execute(self.query_for_agp)

    def prepare_data(self):

        for self.row in self.cursor_read:

            self.id = self.row['id']
            self.unit_notready = self.row['unit_notready']
            self.unit_stdby = self.row['unit_stdby']
            self.unit_attempts = self.row['unit_attempts']
            self.unit_run = self.row['unit_run']
            self.unit_loaded = self.row['unit_loaded']
            self.emerg_stop_blow_cnt = self.row['emerg_stop_blow_cnt']
            self.emrg_stop_cnt = self.row['emrg_stop_cnt']
            self.gas_fuel_flow = self.row['gas_fuel_flow']
            self.gas_fuel_flow_correct = 1000*((self.gas_fuel_flow*0.45)/0.8) #kg/h
            self.hpc_flow_act = self.row['hpc_flow_act']
            self.ufoh = self.row['ufoh']
            self.start_count = self.row['start_count']
            self.dateandtime = self.row['dateandtime']

        return self.id,  self.dateandtime

    def date(self):

        dateandtime = datetime.now()

        return dateandtime

    def usi(self):

        if self.unit_notready == 1:
            self.USI = 1
        elif self.unit_stdby == 1:
            self.USI = 2
        elif self.unit_run == 1 and self.unit_loaded == 0:
            self.USI = 3
        elif self.unit_run == 1 and self.unit_loaded == 1:
            self.USI = 4
        else:
            self.USI = 0
        return self.USI

    def usc(self):
        return self.emerg_stop_blow_cnt + self.emrg_stop_cnt

    def uppt(self):
        self.UPPT = 4000
        return self.UPPT

    def uai(self):

        return round((100 * (self.UPPT -self.ufoh) / self.UPPT), 2)

    def ufti(self):

        return round((self.gas_fuel_flow_correct/self.hpc_flow_act),2)


    def usri(self):

        return round(100 * (self.start_count / (self.unit_attempts)), 2)


    def urri(self):

        return round((100 * (1 - self.ufoh / self.UPPT)), 2)

    def uah(self):

        return self.UPPT - self.ufoh

    def nan(self):
        return None

    def days_hours_minutes(self,td):
        days = td.days * 24
        hours = td.seconds / 3600
        total = days + hours
        return total




















