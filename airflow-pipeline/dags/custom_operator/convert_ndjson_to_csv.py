import logging
import json
import requests
import time
import os
import pandas as pd
import ndjson
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class ConvertNdJsonToCsv(BaseOperator):
    """This operator allows Airflow to convert NDJSON to CSV.
    Args:
        url_source (string): url the data source.
        target_directory (string): directory path where the csv file will stored.
        provide_context (boolean): Whether or not to pass Airflow context value.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments. See GlueJobOperator for more info.
    """

    @apply_defaults
    def __init__(
        self,
        url_source,
        target_directory,
        *args,
        **kwargs
    ):
        super(ConvertNdJsonToCsv, self).__init__(
            *args, 
            **kwargs)

        # Get value from argument
        self.url_source=url_source
        self.target_directory=target_directory

    def __get_data(self, url):
        try:
            res = requests.get(url)
            items = res.json(cls=ndjson.Decoder)
            return items

        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)


    def __convert_to_pandas_df(self, input):
        
        logging.info("Process: Convert to dataframe")
        return pd.DataFrame.from_records(input)

    def __write_to_csv(self, df, target_directory, execution_date):

        if not os.path.exists(target_directory):
            os.makedirs(target_directory) 

        filename = 'result_1_{}.csv'.format(execution_date)
        logging.info("Process: Will write CSV file to directory: {}".format(target_directory))
        df.to_csv(path_or_buf=target_directory + '/' + filename, index=False)
        logging.info("Process: Write CSV file to directory: {} Success!".format(target_directory))

    def execute(self, context):
        
        # get context
        context = {**context.get('dag').default_args, **context}
        logging.info("context={}".format(context))

        execution_date = context["execution_date"].strftime('%Y-%m-%d')
        logging.info("Execution date={}".format(execution_date))

        # get data from url
        result = self.__get_data(self.url_source)

        # convert result as pandas dataframe
        df = self.__convert_to_pandas_df(result)

        # write to csv
        self.__write_to_csv(df, self.target_directory, execution_date)
