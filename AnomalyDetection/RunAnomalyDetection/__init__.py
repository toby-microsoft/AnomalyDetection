import logging
import azure.functions as func

# Azure Data Lake Storage Gen1 filesystem management
from azure.datalake.store import core, lib
from azure.datalake.store.core import AzureDLFileSystem as adls

# functional libraries
import csv
import pandas
import json
import io
import os
import requests
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from datetime import datetime, timedelta, date
from pathlib import Path

# file path to be used with adls
# adjust for utc time
now = datetime.today() - timedelta(days=1, hours=7)
filepath = '/local/Iris.ACM/Dev/' + '{:02d}'.format(now.year) + '/' + '{:02d}'.format(now.month) + '/' +  '{:02d}'.format(now.day) + '/'


def get_credentials(secret):
    subscriptionId = 'c71f08af-8fcd-4f65-b991-143888d0cbd8'
    adlsAccountName = 'iris-acm-prod-c15'
    tenant = '72f988bf-86f1-41af-91ab-2d7cd011db47'
    RESOURCE = 'https://datalake.azure.net/'
    client_id = 'a6a835cf-c106-4ad8-a77e-0285a6e3e447'
    client_secret = secret

    # get the adl credentials
    adlCreds = lib.auth(tenant_id = tenant,
                    client_secret = client_secret,
                    client_id = client_id,
                    resource = RESOURCE)

    adl = core.AzureDLFileSystem(adlCreds, store_name=adlsAccountName)

    return adl

def parse_csv(csv_file):
        
    # Constants for retrieving data from cosmos
    EVENT_DATE = 0
    INTERACTION_ID = 1
    ACTION_ID = 2
    DAILY_COUNT = 4

    raw_data = []
    for line in csv_file.readlines():
        sanitized_data = line.decode('utf-8').strip('\n').strip('\r')
        sanitized_data = sanitized_data.split('\t')
        raw_data.append((sanitized_data[EVENT_DATE], sanitized_data[INTERACTION_ID], 
                        sanitized_data[ACTION_ID], int(sanitized_data[DAILY_COUNT])))

    return raw_data

def write_to_adls(adl, action_and_counts, df):
    
    # cache the actions and its associated counts
    action_count_output = {}
    output = []
    for record in action_and_counts:
        output.append({'InteractionId': str(record[0]), 'ActionId': str(record[1]), 'SuspiciousCount': str(record[2])})

    action_count_output['output'] = output

    json_str = json.dumps(action_count_output)
    with adl.open(filepath + 'action_count_output.txt', 'wb') as action_count_file:
        action_count_file.write(str.encode(json_str))

    df_str = df.to_csv()
    # create new file to in adls to write to
    with adl.open(filepath + 'df_impressions.csv', 'wb') as df_impression_file:
        df_impression_file.write(str.encode(df_str))


def is_saved(adl):
    if adl.exists(filepath + 'action_count_output.txt') and adl.exists(filepath + 'df_impressions.csv'):
        return True
    
    return False


def run_anomaly_detection(adl, threshold):

    # read the cosmos data
    with adl.open(filepath + 'HueristicAbnormalImpressionDay.csv', mode = 'rb', delimiter='\t') as raw_data:
        data = parse_csv(raw_data)

    # write data to data frame to be processed
    df_date, df_interaction_id, df_action_id, df_count = [], [], [], []
    for line in data:
        year, month, day= line[0].split('-')
        observed_date = date(int(year), int(month), int(day)).weekday()
        if observed_date != 5 and observed_date != 6:
            df_date.append(str(line[0]))
            df_interaction_id.append(str(line[1]))
            df_action_id.append(str(line[2]))
            df_count.append(str(line[3]))

    
    cosmos_data = {'Date': df_date, 'InteractionId': df_interaction_id, 
        'ActionId': df_action_id, 'Count': df_count}
    
    df = pd.DataFrame(data=cosmos_data)
    unique_actions = df.ActionId.unique()
    actions_and_count = []

    for actionId in unique_actions:

        filtered_df = df.loc[df['ActionId'] == actionId]
        impressionCount = list(map(int, list(filtered_df['Count'])))
        mean = np.mean(impressionCount)
        std = np.std(impressionCount)
        
        # find the outlier outside of the standard deviation threshold
        outliers=[]
        for y in impressionCount:
            z_score = (y - mean)/std
            if np.abs(z_score) > int(threshold):
                outliers.append(y)


        # find outliers from from the middle values
        dataset = pd.to_numeric(filtered_df['Count'])
   
        sorted(dataset)
        q1, q3= np.percentile(dataset,[25,75])
        iqr = q3 - q1

        lower_bound = q1 -(1.5 * iqr) 
        upper_bound = q3 +(1.5 * iqr) 

        # filter out those outliers
        for num in dataset:
            if num < lower_bound or num > upper_bound:
                if num not in outliers:
                    outliers.append(num)
        
        # don't allow for duplicates
        if len(outliers) != 0:
            actions_and_count.append([list(filtered_df['InteractionId'])[0], actionId, outliers])

    # save the data in adls
    write_to_adls(adl, actions_and_count, df)


def main(req: func.HttpRequest) -> func.HttpResponse:
    secret = json.loads(req.get_body().decode('utf-8'))['secret']
    adl = get_credentials(secret)
    threshold = req.params.get('threshold')

    if threshold:
        if not is_saved(adl):
            run_anomaly_detection(adl, threshold)
        return func.HttpResponse('OK')

    else:
        return func.HttpResponse(
            "Please pass a threshold in the http request body",
            status_code=400
        )