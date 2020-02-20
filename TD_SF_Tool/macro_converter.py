from tool_conf import *
from tkinter import *
import logging
from macro_coversion_config import *
from sql_funcs import *
from change_log import *

logging.basicConfig(
    filename= log_file,
    filemode='w+',
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def create_macro(script):
    try:
        if 'macro' or 'as' in script:
            name_extract = script.partition("macro")[2].partition("as")[0].strip().upper().split('.', 1)
            create_block = conf_map['create macro'].format(name_extract[0], name_extract[1])
            logging.info('create block converted')
            return create_block
        else:
            return 'None'
    except Exception as err:
        logging.error(err)


def merge_into(res):
    try:
        if 'merge' in res:
            merge_var = '{' + 'sqlText:{0}'.format('var_sql_merge_base') + '}'
            merge_script = res.partition("merge")[1] + res.partition("merge")[2].partition(";")[0]
            exec_sf = conf_map['sf_exe_m'].format(merge_var)
            merge_block = shift_date_time(merge_script)
            merge_data = conf_map['merge_into'].format(''.join(merge_block)) + exec_sf
            logging.info('merge block completed')
            return merge_data,merge_script
        else:
            return ['None','None']
    except Exception as error:
        logging.error(error)


def insert_block(script):

    if 'insert' in script and script.count('insert')==1:
        insert_var = '{' + 'sqlText:{0}'.format('var_sql_insert_base') + '}'
        insert_extract = script.partition("insert")[1]+script.partition("insert")[2].partition(");")[0]
        insert_script = conf_map['insert_into'].format(insert_extract) + conf_map['sf_exe_m'].format(insert_var)
        return insert_script
    else:
        return 'None'

def select_block(script):

    if 'select' in script and script.count('select')==1:
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


def update_flow(upd_block):
    updates = []
    update_script = []
    update_collection = []
    name = 'sql_update_var_{0}'


    for i in upd_block.replace('\n', '').replace('\t', '').split('update'):
        if not len(('update' + i)) < 10:
            update_script.append('update' + i)
    for itera ,script in enumerate(update_script):
        update = update_block(script)
        update_collection.append(update)
        after_update_script = script.replace(update, '')
        set_script = set_block(after_update_script)
        update_collection.append(set_script + '\n')
        after_set_script = after_update_script.replace(set_script, '')
        from_script = from_block(after_set_script)
        update_collection.append(from_script + '\n')
        after_from_script = after_set_script.replace(from_script, '')
        where_script = where_block(after_from_script)
        update_collection.append(where_script + '\n')
        collection = [value for value in update_collection if value != 'None\n']
        var_name = name.format(itera)
        update_var = '{' + 'sqlText:{0}'.format(var_name) + '}'
        update_exe = conf_map['sf_exe_update'].format(update_var)
        # sf_script.append(after_from_script)
        update_qry = ''.join(collection)
        update_group = conf_map['update'].format(var_name,update_qry) + update_exe +'\n'
        updates.append(update_group)
    return updates


def query_processor(cont,file_name,op_path):
    try:
        mac = cont.lower()
        cleaned = []
        logging.info('initated conversion for '+file_name)
        for items in mac.split(' '):
            if items.startswith('--' or '\n--'):
                cleaned.append('//'+items)
            else:
                cleaned.append(items)
        script = ' '.join(cleaned)
        rest_script = ''
        op_file_name = file_name + '_converted.txt'
        sf_script = []
        qry_cnt = 0
        if 'as' in script:
            script_block = script.split('as', 1)
            create = script_block[0]
            rest_script = ''.join(script_block[1:])
            create_block = create_macro(create)
            sf_script.append(create_block)
            sf_script.append(conf_map['as_block'])
        if rest_script.replace('\n','').replace('\t','').replace('\r','').startswith('(select' or 'select'):
            select_script = select_block(rest_script)
            sf_script.append(select_script + '\n')
            qry_cnt = qry_cnt + 1
        if rest_script.replace('\n','').replace('\t','').replace('\r','').startswith('(insert' or 'insert'):
            insert_script = insert_block(rest_script)
            sf_script.append(insert_script + '\n')
            qry_cnt = qry_cnt + 1
        if 'BEGIN TRANSACTION'.lower() in rest_script:
            body = rest_script.partition("begin transaction")[2].partition("end transaction")[0]
            extracted = update_flow(body)
            no_script = conf_map['no_into'].format(''.join(extracted))
            sf_script.append(no_script + '\n')
            qry_cnt = qry_cnt + 1
        if qry_cnt == 0:
            merge_block = merge_into(rest_script)
            sf_script.append(merge_block[0] + '\n')
            after_merge_script = rest_script.replace(merge_block[1], '')
            update_collection = update_flow(after_merge_script)
            sf_script.append(''.join(update_collection))

        with open(str(op_path)+'/'+op_file_name , 'w') as f:
            sf_script.append(conf_map['sf_exe_e'])
            sf_converted = [value for value in sf_script if value != 'None\n']
            for item in sf_converted:
                f.write("%s" % item)
            logging.info('completed conversion for ' + op_file_name)
            logging.info('^'*80)

    except Exception as error:
        logging.info(error)