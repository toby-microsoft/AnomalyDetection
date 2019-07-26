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


def get_saved_data(adl):

    # Get the saved processed data
    action_count = {}
    action_count_out = []
    with adl.open(filepath + 'action_count_output.txt', 'rb') as action_count_file:
        action_count = json.load(action_count_file)

    for record in action_count["output"]:
        action_count_out.append([record['InteractionId'], record['ActionId'], record['SuspiciousCount']])
    
    return action_count_out


def main(req: func.HttpRequest) -> func.HttpResponse:
    secret = json.loads(req.get_body().decode('utf-8'))['secret']
    adl = get_credentials(secret)
    HEADERS = {'Content-Type': 'text/html'}
    result = get_saved_data(adl)

    html = '<table border=\"1\"><thead><tr><th>InteractionId</th><th>ActionId</th><th>Suspicious Count</a></th></tr></thead><tbody>'

    for action in result:
        html = html + '<tr><td>' + str(action[0])+ '</td><td><a href=\"https://anomalydetectionproxy.azurewebsites.net/api/GetScatterPlot?actionid=' + str(action[1]) \
        + '\">' + str(action[1]) + '</a></td><td>'+ str(action[2]) +'</td></tr>'

    html = html + '</tbody></table>'

    return func.HttpResponse(html, headers = HEADERS)
