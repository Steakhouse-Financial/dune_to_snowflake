#------------------------------------------------------------------------

# standard modules
import json
from pathlib import Path
import os, sys
import datetime
#sys.path.append((os.path.realpath(__file__)))#

# pip modules
import pandas as pd
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine #needs to be the version of sqlalchemy that supports snowflake
from requests import get, post

# custom modules
from utils import ROOT_DIR


#------------------------------------------------------------------------


def getConfigs():
    configFile = Path(ROOT_DIR) / 'config.json'
    with open(configFile) as json_data_file:
        configs = json.load(json_data_file)
    return configs

BASE_URL = "https://api.dune.com/api/v1/"

def make_api_url(module, action, ID):
    """
    We shall use this function to generate a URL to call the API.
    """
    url = BASE_URL + module + "/" + ID + "/" + action
    return url

def execute_query(query_id):
    """
    Takes in the query ID.
    Calls the API to execute the query.
    Returns the execution ID of the instance which is executing the query.
    """
    url = make_api_url("query", "execute", query_id)
    response = post(url, headers=HEADER)
    execution_id = response.json()['execution_id']
    return execution_id

def execute_query_with_params(query_id, param_dict):
    """
    Takes in the query ID. And a dictionary containing parameter values.
    Calls the API to execute the query.
    Returns the execution ID of the instance which is executing the query.
    """
    url = make_api_url("query", "execute", query_id)
    response = post(url, headers=HEADER, json={"query_parameters" : param_dict})
    execution_id = response.json()['execution_id']
    return execution_id


def get_query_status(execution_id):
    """
    Takes in an execution ID.
    Fetches the status of query execution using the API
    Returns the status response object
    """
    url = make_api_url("execution", "status", execution_id)
    response = get(url, headers=HEADER)
    return response


def get_query_results(execution_id):
    """
    Takes in an execution ID.
    Fetches the results returned from the query using the API
    Returns the results response object
    """
    url = make_api_url("execution", "results", execution_id)
    response = get(url, headers=HEADER)
    return response


def cancel_query_execution(execution_id):
    """
    Takes in an execution ID.
    Cancels the ongoing execution of the query.
    Returns the response object.
    """
    url = make_api_url("execution", "cancel", execution_id)
    response = get(url, headers=HEADER)
    return response

def getAPIKey():
    configs = getConfigs()
    k = configs['dune']['apikey']
    return k

def get_dune_df(query_id, parameters):
    execution_id = execute_query_with_params(query_id, parameters)
    state = get_query_status(execution_id).json()['state']
    duration = 0

    while state != 'QUERY_STATE_COMPLETED':
        time.sleep(60)
        state = get_query_status(execution_id).json()['state']
        duration = duration + 60
        print(str(duration/60)+' minutes elapsed')

    response = get_query_results(execution_id)
    df = pd.DataFrame(response.json()['result']['rows'])
    return df

def getSnowflakeUsername():
    configs = getConfigs()
    u = configs['snowflake']['username']
    return u

def getSnowflakePassword():
    configs = getConfigs()
    p = configs['snowflake']['password']
    return p

def getSnowflakeAccount():
    configs = getConfigs()
    a = configs['snowflake']['account']
    return a

def scrubLatestDate(tablename):
    user = getSnowflakeUsername()
    pw = getSnowflakePassword()
    account = getSnowflakeAccount()
    if user == 'username' or pw == 'password':
        raise Exception("Snowflake username / password entry is invalid. Please update config.json")
    engine=create_engine('snowflake://{user}:{password}@{account}/USER_TB/PUBLIC?warehouse=XS&role=PUBLIC'.format(user=user,password=pw,account=account))
    result = engine.execute('SELECT MAX(DATE(ts)) FROM {tablename}'.format(tablename=tablename))
    max_dt = result.scalar()
    engine.execute('DELETE FROM {tablename} WHERE DATE(ts) = \'{max_dt}\''.format(tablename=tablename, max_dt=max_dt))
    return max_dt.strftime("%Y-%m-%d")


def uploadToSnowflake(df, tablename):
    user = getSnowflakeUsername()
    pw = getSnowflakePassword()
    account = getSnowflakeAccount()
    if user == 'username' or pw == 'password':
        raise Exception("Snowflake username / password entry is invalid. Please update config.json")
    engine=create_engine('snowflake://{user}:{password}@{account}/USER_TB/PUBLIC?warehouse=XS&role=PUBLIC'.format(user=user,password=pw,account=account))
    df.columns = map(lambda x: str(x).upper(), df.columns)
    df.to_sql(tablename, engine, if_exists='replace', index=False, method=pd_writer) #if_exists can also be 'append' to append the data to the existing table
    print('done uploading to snowflake')

def uploadToSnowflakeAppend(df, tablename):
    user = getSnowflakeUsername()
    pw = getSnowflakePassword()
    account = getSnowflakeAccount()
    if user == 'username' or pw == 'password':
        raise Exception("Snowflake username / password entry is invalid. Please update config.json")
    engine=create_engine('snowflake://{user}:{password}@{account}/USER_TB/PUBLIC?warehouse=XS&role=PUBLIC'.format(user=user,password=pw,account=account))
    df.columns = map(lambda x: str(x).upper(), df.columns)
    df.to_sql(tablename, engine, if_exists='append', index=False, method=pd_writer) #if_exists can also be 'replace' to replace the data in an existing table
    print('done uploading to snowflake')



if __name__ == '__main__':
    # initialize client
    user = getSnowflakeUsername()
    pw = getSnowflakePassword()


    API_KEY = getAPIKey()
    HEADER = {"x-dune-api-key" : API_KEY}

    if user == 'username' or pw == 'password' or API_KEY == 'apikey':
        raise Exception("Dune username / password or apikey entry is invalid. Please update config.json")

    begin_dt = scrubLatestDate('maker_tx_level_accounting')
    parameters = {"begin_dt" : begin_dt, "end_dt" : "2099-12-31"}

    df = get_dune_df("2030153", parameters)

    df['ts']=pd.to_datetime(df['ts']) #handling of ts
    uploadToSnowflakeAppend(df = df, tablename = 'maker_tx_level_accounting')
