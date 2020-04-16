import re
from macro_coversion_config import *
import logging


def shift_date_time(merge_script):
    merge_block = []
    merge = merge_script.split('\n')
    for i, ite in enumerate(merge):
        if re.findall('shift_info.shiftstartdatetime', ite):
            col_name = ite.split('+')[0].strip()
            field = re.search(r'cast([^/]+)', ''.join(ite.split('+')[1])).group(1).replace('(', '').strip()
            update_col = conf_map['date_add'].format(field, col_name) + ' as ' + field + '_ts ,'
            logging.info(update_col)
            merge_block.append(update_col + '\n')

        else:
            merge_block.append(ite + '\n')

    return merge_block

def merge_into(res):
    try:
        if 'merge' in res:
            merge_var = '{' + 'sqlText:{0}'.format('var_sql_merge_base') + '}'
            merge_script = res.partition("merge")[1] + res.partition("merge")[2].partition(";")[0]
            exec_sf = conf_map['sf_exe_m'].format(merge_var)
            merge_block = shift_date_time(merge_script)
            # merge_data = conf_map['merge_into'].format(''.join(merge_block)) + exec_sf
            logging.info('merge block completed')
            return merge_block,merge_script
        else:
            return ['None','None']
    except Exception as error:
        logging.error(error)


def insert_block(script):

    if 'insert' in script:
        insert_var = '{' + 'sqlText:{0}'.format('var_sql_insert_base') + '}'
        insert_extract = script.partition("insert")[1]+script.partition("insert")[2].partition(");")[0]
        insert_script = conf_map['insert_into'].format(insert_extract) + conf_map['sf_exe_m'].format(insert_var)
        return insert_script
    else:
        return 'None'

def select_block(script):

    if 'select' in script:
        select_var = '{' + 'sqlText:{0}'.format('var_sql_select_base') + '}'
        select_extract = script.partition("select")[1]+script.partition("select")[2].partition(");")[0]
        select_script = conf_map['select'].format(select_extract)+ conf_map['sf_exe_m'].format(select_var)
        return select_script
    else:
        return 'None'


def update_block(block):
    try:
        if re.findall('update', block):

            update = block.partition("update")[1] + block.partition("update")[2].partition("set")[0]
            update_script = update
            logging.info('update block')
            return update_script
        else:
            return 'None'
    except Exception as error:
        logging.error(error)

def set_block(block):
    try:
        if re.findall('set',block):
            set_script = block.partition("set")[1] + block.partition("set")[2].partition("where")[0]
            logging.info('set block')
            return set_script
        else:
            return 'None'
    except Exception as error:
        logging.error(error)

def from_block(block):
    try:
        if re.findall('from',block):
            from_script = block.partition("from")[1] + block.partition("from")[2].partition("where")[0]
            logging.info('from block')
            return from_script
        else:
            return 'None'
    except Exception as error:
        logging.error(error)

def where_block(block):
    try:
        if re.findall('where',block):
            where_script = block.partition("where")[1] + block.partition("where")[2].partition(");")[0]
            logging.info('where block completed')
            return where_script
        else:
            return 'None'
    except Exception as error:
        logging.error(error)