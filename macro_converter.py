import os
import re
import logging
from macro_coversion_config import *
from change_log import *

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def create_macro(k,v,block):
    try:
        rm_space = re.compile(r'\s+')
        pd = list(filter(lambda x: k in re.sub(rm_space, ' ', x), block))[0].split('as', 1)
        mac_name = ''.join(list(filter(lambda x: k in re.sub(rm_space, ' ', x), pd))).split('macro')[-1].replace('\n', '')
        macro_name = mac_name.split('.')
        converted = v.format(macro_name[0].strip().upper(),macro_name[1].strip().upper())
        logging.info(converted)
        return converted
    except Exception as err:
        print(err)

def replace_macro(k,v,block):
    try:
        rm_space = re.compile(r'\s+')
        pd = list(filter(lambda x: k in re.sub(rm_space, ' ', x), block))[0].split('as', 1)
        mac_name = ''.join(list(filter(lambda x: k in re.sub(rm_space, ' ', x), pd))).split('macro')[-1].replace('\n', '')
        macro_name = mac_name.split('.')
        converted = v.format(macro_name[0].strip().upper(),macro_name[1].strip().upper())
        logging.info(converted)
        return converted
    except Exception as err:
        print(err)

def merge_into(k,v,block):
    try:
        merge = k.split('\n')
        merge_block = []
        for i, ite in enumerate(merge):
            if re.findall('shiftstartdatetime', ite):
                col_name = ite.split('+')[0].strip()
                field = ''.join(k).split('\n')[i - 1].replace(',', '').strip()
                update_col = conf_map['date_add'].format(field,col_name) + ' as ' +field +'_ts ,'
                logging.info(update_col)
                merge_block.append(update_col+'\n')
            else:
                logging.info('No dateadd func necessary')
                merge_block.append(ite+'\n')

        blocks = []
        if re.findall('update', block):
            blocks.append('\nvar_sql_logical_delete_capture = ' +"'" + 'update' + block.split('from',1)[0].split('update')[-1])
            logging.info('update block')
        if re.findall('set',block):
            blocks.append('set' + block.split('from',1)[-1].split('set')[-1].split('where')[0])
            logging.info('set block')
        if re.findall('from',block):
            blocks.append(' from'+ block.split('from',1)[-1].split('set')[0] )
            logging.info('from block')
        if re.findall('where',block):
            blocks.append('where' +block.split('from',1)[-1].split('set')[-1].split('where')[-1]+ "'")
            logging.info('where block')
        as_block = conf_map['as_block']
        merge_sec = v.format(''.join(merge_block))
        exe_macro = conf_map['sf_exe_m']
        ot_blocks = ''.join(blocks)
        exe_ot = conf_map['sf_exe_b']
        end =  conf_map['sf_exe_e']
        converted = op_log+as_block+ merge_sec + ';' +'\n\n'+ exe_macro + '\n\n'+ ot_blocks+'\n'+exe_ot+'\n\n'+ end
        logging.info(converted)
        return converted
    except Exception as err:
        print(err)

def new_keys(block):
    keyw = []
    inp_keys = []
    resp_keys = []
    for i in block:
        keyw.append(i.split(' ', 1)[0] + ' macro')
    for kys in list(conf_map.keys()):
        inp_keys.append(" ".join(kys.split()))
    for h in keyw:
        if h not in inp_keys:
            resp_keys.append(list(filter(lambda x: h in x, block)))
    with open('td_rs_new_keys.txt', 'w') as f:
        for item in resp_keys:
            f.write("%s\n" % item)
    return resp_keys

def query_processor(file):
    with open(file, 'r') as inp:
        op = inp.readlines()
        mac = ''.join(op)
    cleaned = []
    for items in mac.split(' '):
        if len(items) >=1:
            cleaned.append(items)
    clean = ' '.join(cleaned)
    block = ''.join(clean.split(');')).replace('\n','\n ').replace('"','').split(';')
    op_file = file.split('\\')[-1].split('.')[0]+'_converted.txt'
    query_resp = []
    merge = 'merge' + mac.split('merge')[1].split(');',1)[0]
    non_merge = mac.split('merge')[1].split(');',1)[1]
    for k, v in conf_map.items():
        if k == 'create macro':
            c_macro = create_macro(k, v,block)
            query_resp.append(c_macro)
        elif k == 'replace macro':
            c_macro = replace_macro(k, v,block)
            query_resp.append(c_macro)
        elif k == 'merge into':
            m_macro = merge_into(merge,v,non_merge)
            query_resp.append(m_macro)
    with open(op_file , 'w') as f:
        for item in query_resp:
            f.write("%s\n" % item)
    logging.info('completed conversion for ' + file.split('\\')[-1].split('.')[0])
    new_keys(block)

if __name__ == '__main__':

    src_path = "C:\\Users\\45444\\PycharmProjects\\TD_FS_migrator\\files\\extracted\\"
    inp_list = os.listdir(src_path)
    for files in inp_list:
        file = src_path+files
        query_processor(file)
    logging.info('completed')
    # query_processor(r'C:\Users\45444\PycharmProjects\TD_FS_migrator\files\macro\Teradata Actual Macro.txt')

# ''.join(op[op.index('using \n'):op.index('when matched then \n')]).replace('\n','').replace('\t','')