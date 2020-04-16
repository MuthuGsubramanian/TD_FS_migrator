#
import pandas as pd
import logging
from tool_conf import *
from macro_coversion_config import conf_map
from sql_funcs import *
import os
import re

logging.basicConfig(
    filename= log_file,
    filemode='w+',
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def resource_path(relative):
    return os.path.join(
        os.environ.get(
            "_MEIPASS2",
            os.path.abspath(".")
        ),
        relative
    )

def mapping(db_script,conf):

    create = {}
    db_names = []
    text = ''
    df_tbl = 'table_mapping.xlsx'
    df = pd.read_excel(conf.name)
    db_map = df.apply(lambda x: x.astype(str).str.lower())
    old_name = re.findall(r'(\w+[.]\w+)', db_script)
    for td_dbs in old_name:
        db_names.append(td_dbs.split('.')[0].strip())
    filtered_dbs = db_map.loc[db_map['DatabaseName'].isin(db_names)].to_dict(orient='records')
    for dbs in filtered_dbs:
        td_db = dbs['DatabaseName'].lower().strip()
        sf_db = dbs['Database by subject'].lower().strip()
        create[td_db] = sf_db
    for td,sf in create.items():
        text = db_script.replace(td,sf)
        logging.info('Table mapping updated from '+ td + ' to '+ sf)
    return text


def update_flow(upd_block):
    updates = []

    if 'update' in upd_block:
        updates.append(upd_block.partition("update")[1] +' ' +upd_block.partition("update")[2].partition("from")[0].strip()+ '\n')
    if 'set' in upd_block:
        updates.append(upd_block.partition("set")[1] +' ' + upd_block.partition("set")[2].partition("where")[0].strip()+ '\n')
    if 'from' in upd_block:
        updates.append(upd_block.partition("from")[1] +' ' +upd_block.partition("from")[2].partition("set")[0].strip()+ '\n')
    if 'where' in upd_block:
        updates.append(upd_block.partition("where")[1] +' ' +upd_block.partition("where")[2].partition(");")[0].strip()+ '\n')

    logging.info('update logic processed')
    update_script = ''.join(updates)

    return update_script