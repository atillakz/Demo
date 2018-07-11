#!/home/user/anaconda3/bin/python
import json
import requests

header = {'Content-Type': 'application/json', \
                  'Accept': 'application/json'}

import pandas as pd

import pandas as pd
from influxdb import DataFrameClient
from sklearn.externals import joblib
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import time
import numpy as np


class Online_predictor():

    def __init__(self, host, port, user, password, db_name, query_body):

        self.host = host

        self.port = port

        self.user = user

        self.password = password

        self.db_name = db_name

        self.query_body = query_body

    def get_data_from_influx(self):

        self.client = DataFrameClient(self.host, self.port, self.user, self.password, self.db_name)

        self.data = self.client.query(self.query_body)

        return self.data


    def read_data(self):

        self.new_data = dict(self.data)


        for i in self.new_data:

            self.my_data = self.new_data[i]


    def prepare_data(self):

        self.my_data = self.my_data.reset_index()

        self.my_data = self.my_data.dropna()

        return self.my_data

    def show(self):

        return  self.my_data['gas_fuel_flow_x']

