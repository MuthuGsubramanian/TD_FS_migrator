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

    pd = list(filter(lambda x: k in x, block))
    if len(pd)== 0:
        pd = list(filter(lambda x: '\ncreate   macro' in x, block))
        macro_name = pd[0].split('(',1)[0].split('  ',2)[-1].split('\n')[0]
        converted = v.format(macro_name.upper())
    else:
        macro_name = pd[0].split('(', 1)[0].split('  ', 2)[-1].split('\n')[0]
        converted = v.format(macro_name.upper())
    return converted


def merge_into(k,v,block):
    merge_block = block[0].split('(',1)[1]
    d = ''.join(block).split("  d")[-1]
    update = 'var_sql_logical_delete_capture = ' +d.split(('set'))[0].split('from')[0]
    if 'set' in d:
        set = 'set\n' + d.split(('set'))[1]
        from_b = 'from' +"'"+ d.split(('set'))[0].split('from')[1] +"'"
        converted = conf_map['as_block'] + v.format(merge_block) + ';' \
                    + '\n\n' + conf_map['sf_exe_b'] + '\n\n' + update + set + from_b +conf_map['sf_exe_m']+ conf_map['sf_exe_e']
    else:
        converted =conf_map['as_block']+ v.format(merge_block)+';' \
               +'\n\n'+ conf_map['sf_exe_b'] + '\n\n' + update + conf_map['sf_exe_e']

    return converted

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
    rm_space = re.compile(r'\s+')
    with open(file, 'r') as inp:
        op = inp.readlines()
        mac = re.sub(rm_space,' ',''.join(op))
    block = ''.join(mac.split(');')).replace('\n','\n ').replace('"','').split(';')
    op_file = file.split('\\')[-1].split('.')[0]+'_converted.txt'
    query_resp = []
    for k, v in conf_map.items():
        if k == 'create macro':
            c_macro = create_macro(k, v,block)
            query_resp.append(c_macro)
        elif k == 'merge into':
            m_macro = merge_into(k,v,block)
            query_resp.append(m_macro)
    query_resp.insert(0,op_log)
    with open(op_file , 'w') as f:
        for item in query_resp:
            f.write("%s\n" % item)
    new_keys(block)
        # else:

if __name__ == '__main__':

    src_path = "C:\\Users\\45444\\PycharmProjects\\TD_FS_migrator\\files\\macro\\"
    inp_list = os.listdir(src_path)
    for files in inp_list:
        file = src_path+files
        query_processor(file)
    # query_processor(r'C:\Users\45444\PycharmProjects\TD_FS\playground\files\macro\m_lh2_config_equip_status_b.sql')

# ''.join(op[op.index('using \n'):op.index('when matched then \n')]).replace('\n','').replace('\t','')