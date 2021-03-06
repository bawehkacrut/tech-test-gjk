import re
import os
import pandas as pd
import pandasql as pdsql
import numpy as np
import logging
from datetime import datetime, timedelta, date
from airflow.exceptions import AirflowException



def run(**kwargs):
    # Print input parameters
    execution_date = kwargs["execution_date"].strftime('%Y-%m-%d')
    logging.info("Execution datetime={}".format(execution_date))
    logging.info("Params={}".format(kwargs["params"]))

    parent_df = pd.DataFrame([['a', np.nan],
                           ['b', np.nan],
                           ['c', 'b'],
                           ['e', np.nan],
                           ['f', 'e'],
                           ['g', 'f'],
                           ['h', 'g']],
                  columns=['node', 'parent'])
    logging.info("Parent Dataframe: \n{}".format(parent_df))

    result_a = """
            WITH RECURSIVE node_with_roots AS (
            SELECT node, node as root
            FROM parent_df
            WHERE parent IS NULL

            UNION ALL

            SELECT pd.node, nwr.root
            FROM parent_df pd, node_with_roots nwr
            WHERE pd.parent = nwr.node
            )
            SELECT node, root FROM node_with_roots
            """
                    

    df_result_a = pdsql.sqldf(result_a, locals())

    logging.info("Dataframe Result A: \n{}".format(df_result_a))


    