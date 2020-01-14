import os
import re
import logging
from TD_SF_Project.view_conversion_config import *
from TD_SF_Project.change_log import *

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
        # logging.info(converted)
        return converted
    except Exception as err:
        logging.info(err)

def replace_macro(k,v,block):
    try:
        rm_space = re.compile(r'\s+')
        pd = list(filter(lambda x: k in re.sub(rm_space, ' ', x), block))[0].split('as', 1)
        mac_name = ''.join(list(filter(lambda x: k in re.sub(rm_space, ' ', x), pd))).split('macro')[-1].replace('\n', '')
        macro_name = mac_name.split('.')
        converted = v.format(macro_name[0].strip().upper(),macro_name[1].strip().upper())
        # logging.info(converted)
        return converted
    except Exception as err:
        logging.info(err)

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
                merge_block.append(ite+'\n')

        blocks = []
        if re.findall('update', block):
            blocks.append('\nvar_sql_logical_delete_capture = ' +"`" + 'update' + block.split('from',1)[0].split('update')[-1])
            logging.info('update block')
        if re.findall('set',block):
            blocks.append('set' + block.split('from',1)[-1].split('set')[-1].split('where')[0])
            logging.info('set block')
        if re.findall('from',block):
            blocks.append(' from'+ block.split('from',1)[-1].split('set')[0] )
            logging.info('from block')
        if re.findall('where',block):
            blocks.append('where' +block.split('from',1)[-1].split('set')[-1].split('where')[-1].split(');')[0]+ "`;")
            logging.info('where block')
        as_block = conf_map['as_block']
        merge_sec = v.format(''.join(merge_block))
        exe_macro = conf_map['sf_exe_m']
        ot_blocks = ''.join(blocks)
        exe_ot = conf_map['sf_exe_b']
        end =  conf_map['sf_exe_e']
        converted = '\n'+op_log+as_block+ merge_sec + ';' +'\n\n'+ exe_macro + '\n\n'+ ot_blocks+'\n'+exe_ot+'\n\n'+ end
        return converted
    except Exception as err:
        logging.info(err)


def query_processor(file):
    try:
        rm_space = re.compile(r'\s+')
        with open(file, 'r') as inp:
            op = inp.readlines()
            mac = ''.join(op)
        cleaned = []
        for items in mac.split(' '):
            if len(items) >=1:
                cleaned.append(items)
        clean = ' '.join(cleaned)
        block = ''.join(clean.split(');')).replace('\n','\n ').replace('"','').split(';')
        op_file = file.split('\\')[-1].split('.')
        del (op_file[-1])
        op_file_name = ''.join(op_file).strip() + '_converted.txt'
        query_resp = []
        created = mac.split('select',1)
        create_view = list(filter(lambda x: 'create' in x.lower() or 'replace' in x.lower(), created))[0].split('\n')

        for k, v in conf_map.items():
            if k == 'create view':
                name = None
                view_name = list(filter(lambda x: 'CREATE SET' in re.sub(rm_space, ' ', x) or
                                                  'CREATE VIEW' in re.sub(rm_space, ' ', x) or
                                                  'create view' in re.sub(rm_space, ' ', x) or
                                                  'replace view' in re.sub(rm_space, ' ', x),create_view))
                c_macro = view_name[0].split(' ')
                for names in c_macro:
                    if '.' in names:
                        name = names
                db = 'FREEPORT'
                f = conf_map['create view'].format(db,name)
                query_resp.append(f)
                sql = '\nAS \n' + 'select' + ''.join(created[1:])
                query_resp.append(sql)

        with open('files/converted/'+op_file_name , 'w') as f:
            for item in query_resp:
                f.write("%s" % item)
            logging.info('completed conversion for ' + file.split('\\')[-1])
    except Exception as error:
        logging.info(error)


if __name__ == '__main__':

    src_path = "/files/views\\"
    op_path = "/files/converted\\"
    inp_list = os.listdir(src_path)
    for files in inp_list:
        file = src_path+files
        query_processor(file)
    logging.info('completed')
    # query_processor(r'C:\Users\45444\PycharmProjects\TD_FS_migrator\Teradata Actual View.txt')

# ''.join(op[op.index('using \n'):op.index('when matched then \n')]).replace('\n','').replace('\t','')