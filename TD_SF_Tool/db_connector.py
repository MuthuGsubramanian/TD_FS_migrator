import os
import pandas as pd
import teradatasql
from snowflake.connector import connect
import os
import pyodbc
from pyhive import hive
os.environ['ODBC'] = 'Hive.dsn'

from db_conf import *

def snow_conn(qry):

    conn = connect(
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
    return df

def teracon(query):

    # thost, tusername, tpassword = 'phelps2', 'edw_etl_sandbox', 'Edw$andb0x'
    # query = "select * from edw_target.lh2_app_config_b where app_config_id in (1967,844);"
    with teradatasql.connect(host=db_td['td_host'], user=db_td['td_user'], password=db_td['td_pwd']) as connect:
        df = pd.read_sql(query, connect)
        print(df)
        return df

def hive_con(query):

    cnx = pyodbc.connect(dsn = 'Hive', autocommit ='true')
    df = pd.read_sql(query, cnx)
    # print(df)
    return df

print(snow_conn("""select  top 10 * from dev_crusher.app.cve_ramp_tertiary_crushing_hpgr_gearbox_polysius_1s
where localdate ='2019-12-11' order by localdate;"""))
# teracon("show macro EDW_ETL_VIEW.js_locations_saf_tm")
# print(hive_con("""select top 10 * from app_crusher.cve_ramp_tertiary_crushing_hpgr_gearbox_polysius_1s
# where localdate ='2019-12-11' order by localdate;"""))

