from tool_conf import *
import pandas as pd
import re
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
        variables = []
        if 'macro' or 'as' in script:
            name_extract = script.partition("macro")[2].partition("as ")[0].strip().upper().split('.', 1)
            if '(' in name_extract[-1]:
                tbl_name = name_extract[-1].partition('(')[0]
                func_name = name_extract[-1].partition('(')[1]+name_extract[-1].partition('(')[2]
                if func_name:
                    # r = re.compile(r'(?:[^,(]|\([^)]*\))+')
                    # for i in r.findall(func_name):
                    #     f = i.strip().split(' ', 1)
                    for i in func_name.split(','):
                        g = i.strip().split(' ')
                        variables.append(g[0].lower().replace('(', ''))
                        # variables.append(f[0].lower().replace('(', ''))
                create_block = conf_map['create macro'].format(name_extract[0], tbl_name,func_name)
            else:
                create_block = conf_map['create macro'].format(name_extract[0], name_extract[1],'()')

            logging.info('create block converted')
            return create_block,variables
        else:
            return 'None'
    except Exception as err:
        logging.error(err)



def replace_funcs(funcs,script):
    dct = {}
    if funcs:
        for i in funcs:
            if i in script:
                dct[':' + i] = '`+' + i + '+`'

        dct = dict((re.escape(k), v) for k, v in dct.items())
        pattern = re.compile("|".join(dct.keys()))
        text = pattern.sub(lambda m: dct[re.escape(m.group(0))], script)
        return text
    else:
        return script

def mapping(db_script):
    create = ''
    db_map = pd.read_excel('table_mapping.xlsx').to_dict(orient='records')
    for dbs in db_map:
        td_db = dbs['DatabaseName'].lower()
        sf_db = dbs['Database by subject']
        if td_db in db_script:
            create = db_script.replace(td_db, sf_db)
            return create
        else:
            continue
    if create == '':
        return db_script



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
        var = None
        qry_cnt = 0
        if 'as' in script:
            script_block = re.split('as |as\n',script,1)
            create = mapping(script_block[0])
            after_create = ''.join(script_block[1:])
            create_block = create_macro(create)
            var = create_block[1]
            rest_script = replace_funcs(var,after_create)
            sf_script.append(create_block[0])
            sf_script.append(op_log+'\n\n')
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
            extracted = conf_map['no_into'].format(body)
            # no_script = conf_map['no_into'].format(''.join(extracted))
            sf_script.append(extracted + '\n')
            sf_script.append(conf_map['sf_exe_no'])
            qry_cnt = qry_cnt + 1
        if qry_cnt == 0:
            merge_block = conf_map['no_into'].format(rest_script)
            sf_script.append(merge_block + '\n')
            sf_script.append(conf_map['sf_exe_no'])

        with open(str(op_path)+'/'+op_file_name , 'w') as f:
            sf_script.append(conf_map['sf_exe_e'])
            sf_converted = [value for value in sf_script if value != 'None\n']
            for item in sf_converted:
                f.write("%s" % item)
            logging.info('completed conversion for ' + op_file_name)
            logging.info('^'*80)

    except Exception as error:
        logging.info(error)