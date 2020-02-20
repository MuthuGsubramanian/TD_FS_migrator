import os
import pandas as pd
import teradatasql
import snowflake.connector
from pyhive import hive
from db_conf import *

def connect(qry):

    conn = snowflake.connector.connect(
                    user= db_sf['sf_user'],
                    password=db_sf['sf_pwd'],
                    account=db_sf['sf_acct'],
                    warehouse=db_sf['sf_wareh'],
                    database=db_sf['sf_db'],
                    schema=db_sf['sf_schema']
                    )

    # Create cursor
    cur = conn.cursor()
    # Execute SQL statement
    cur_exe = cur.execute(qry)
    df = pd.DataFrame.from_records(iter(cur_exe), columns=[x[0] for x in cur_exe.description])
    # Fetch result
    print(df)

def teracon(query):

    # thost, tusername, tpassword = 'phelps2', 'edw_etl_sandbox', 'Edw$andb0x'
    # query = "select * from edw_target.lh2_app_config_b where app_config_id in (1967,844);"

    with teradatasql.connect(host=db_td['td_host'], user=db_td['td_user'], password=db_td['td_pwd']) as connect:
        df = pd.read_sql(query, connect)
        print(df)

def hive_con(query):
    conn = hive.Connection(host=db_hv['hv_host'],
                           port=2181,
                           username="mgnangu",
                           password="Nmnesan*11",
                           auth='LDAP')

    df = pd.read_sql(query, conn)
    print(df)


connect('select * from dev_general.target.lh2_app_config_b where app_config_id in (1967,844);')
teracon("select * from edw_target.lh2_app_config_b where app_config_id in (1967,844);")
