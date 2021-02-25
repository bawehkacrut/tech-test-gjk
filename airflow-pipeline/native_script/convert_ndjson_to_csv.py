import logging
import json
import requests
import time
import os
import pandas as pd
import ndjson
from datetime import datetime, timedelta, date
from pathlib import Path



def get_data(url):
    try:
        res = requests.get(url)
        items = res.json(cls=ndjson.Decoder)
        return items

    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)


def convert_to_pandas_df(input):
    
    logging.info("Process: Convert to dataframe")
    return pd.DataFrame.from_records(input)

def write_to_csv(df, target_directory, execution_date):

    if not os.path.exists(target_directory):
        os.makedirs(target_directory) 

    filename = 'result_1_{}.csv'.format(execution_date)
    logging.info("Process: Will write CSV file to directory: {}".format(target_directory))
    df.to_csv(path_or_buf=target_directory + '/' + filename, index=False)
    logging.info("Process: Write CSV file to directory: {} Success!".format(target_directory))

if __name__ == '__main__':
    
    # set some varable
    url_source = 'https://storage.googleapis.com/andika_dev/q1_data_source.json'
    execution_date = datetime.now().strftime('%Y-%m-%d')

    parent_path = Path().resolve()
    target_directory = os.path.join(parent_path, "result/number_1")

    logging.info("Execution date={}".format(execution_date))

    # get data from url
    result = get_data(url_source)

    # convert result as pandas dataframe
    df = convert_to_pandas_df(result)

    # write to csv
    write_to_csv(df, target_directory, execution_date)

