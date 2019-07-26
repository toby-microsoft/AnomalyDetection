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


def create_figure(action_id, df):
    selected_df = df.loc[df['ActionId'] == int(action_id)]
    
    #for index, row in selected_df.iterrows():
    selected_df['Date'] = selected_df['Date'].map(lambda x: x.lstrip('2019-'))

    print(selected_df)

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.scatter(selected_df['Date'], selected_df['Count'])
    ax.set_title('Action Id: ' + action_id)
    ax.set_xlabel('Date')
    ax.set_ylabel('Count')

    fig.set_figwidth(15)
    return fig


def get_saved_data(adl):

    # Get the saved dataframe 
    df = pd.DataFrame()
    with adl.open(filepath + 'df_impressions.csv', 'rb') as df_impression_file:
        df = pd.read_csv(df_impression_file)
    
    return df


def main(req: func.HttpRequest) -> func.HttpResponse:
    secret = json.loads(req.get_body().decode('utf-8'))['secret']
    adl = get_credentials(secret)
    
    action_id = req.params.get('actionid')
    fig = create_figure(action_id, get_saved_data(adl))
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)

    return func.HttpResponse(output.getvalue(), mimetype='image/png')
    # return func.HttpResponse(output.getvalue())

