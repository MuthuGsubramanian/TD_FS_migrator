
from pro_conversion_cofig import *
import re
import os
import logging

logging.basicConfig(
    filename='logs.txt',
    filemode='a',
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


logging.info('create block converted')



def proc_converter(content,file_name,op_dir):
    try:
        logging.info('--------------------------------')
        logging.info('initated conversion for create or replace proc --'+file)

        # with open(file, 'r') as inp:
        #     content = inp.read()
        # op_file = file.split('\\')[-1].split('.')[0]+'_converted.txt'
        op_file = op_dir +'/'+ file_name +'_converted.txt'
        print(op_file)
        #comment =  re.sub(r'/\*[\s\S]*?\*/',' ',content)
        
        pattern2 = re.compile(r'/\*[\s\S]*?\*/') 
        match2 = pattern2.finditer(content)
        commentvalue = []
        for match in match2:
            commentvalue.append(content[match.start() : match.end()])
        print('Comment Value', commentvalue)
        content = re.sub(r'/\*[\s\S]*?\*/',' ',content)
        content = re.sub(' +',' ',content)
        content = re.sub('--[\s\S]*?\\n','',content)
        var = 1
        split_content = content.split(';')
        ifsplit = []
        for i in split_content:
            if i.strip().startswith('if') or i.strip().startswith('else'):
                item = i.split('\n')
                for each in item:
                    if each.strip() != '':
                        ifsplit.append(each)
            else:
                ifsplit.append(i.strip())
        print(len(ifsplit))
        split_content = ifsplit
        query_resp = []
        for item in split_content:
            for k, v in conf_map.items():
                if k == 'replace procedure' and ('create procedure' in item or 'replace procedure' in item):
                    logging.info('Create Procedure converted')

                    print('replace procedure')
                    item = item.strip()
                    proc_name = item.split('(')[0].strip()
                    proc_name = proc_name.split(' ')[-1]
                    parameters = re.findall('\([\s\S]*begin',item)
                    proc = conf_map['replace procedure'].format(proc_name,parameters[0].replace('begin','')[1:-2])
                    query_resp.append(proc)
                    query_resp.append(commentvalue[0])
                    query_resp.append(conf_map['try'])
                if k == 'set' and item.strip().startswith('set') and 'select' in item:
                    logging.info('set with select converted')
                    print('set')
                    item = item.strip()
                    varname = item.split(' ')[1]
                    varname1 =  varname + '1'
                    querystring = re.findall(r'\([\s\S]*\)',item)
                    
                    #parameters = split_content[0].split('(')[1]
                    proc = conf_map['set'].format(name=varname,name1=varname1,query= querystring[0])
                    query_resp.append(proc)
                if k == 'delete' and item.strip().startswith('delete') and 'where' not in item:
                    logging.info('delete  without where converted')
                    print('delete')
                    item = item.strip()
                    split = item.split(' ')
                    varname = ' '.join(split[1:])   
                    var = var+1
                    #parameters = split_content[0].split('(')[1]
                    proc = conf_map['delete'].format(name=var,value=varname)
                    query_resp.append(proc)
                if k == 'deletewhere' and item.strip().startswith('delete') and 'where' in item:
                    logging.info('delete with where converted')
                    print('deletewhere')
                    item = item.strip()
                    var = var+1
                    #parameters = split_content[0].split('(')[1]
                    proc = conf_map['deletewhere'].format(name=var,value=item.strip())
                    query_resp.append(proc)
                if k == 'call' and item.strip().startswith('call'):
                    logging.info('Call  converted ')
                    print('call')
                    item = item.strip()
                    split = item.split(' ')
                    querystring = re.findall(r'\([\s\S]*?\)',item)
                    querystring = re.sub('\|\|','',querystring[0])
                    var = var+1
                    proc = conf_map['call'].format(name=var,value=querystring)
                    
                if k == 'merge' and item.strip().startswith('merge'):
                    logging.info('Merge converted')

                    print('merge')
                    item = item.strip()
                    querystring = re.findall(r'merge[\s\S]*',item)
                    
                    querystring = re.sub('\|\|','',querystring[0])
                    var = var+1
                    proc = conf_map['merge'].format(name=var,value=querystring)
                    query_resp.append(proc)
                if k == 'update' and item.strip().startswith('update'):
                    logging.info('Update converted')
                    print('update')
                    item = item.strip()
                    querystring = item.strip()
                    split = item.strip().split(' ')
                    var = var+1
                    proc = conf_map['update'].format(value= querystring , name = var)
                    query_resp.append(proc)
            if item.strip().startswith('if') or item.strip().startswith('else') :
                logging.info('if or else converted')
                query_resp.append(item.strip())
            if item.strip().startswith('then'):
                logging.info('then converted')
                query_resp.append('{')
            if item.strip().startswith('end if'):
                logging.info('End IF converted')
                query_resp.append('}')
            if item.strip().startswith('set') and 'select' not in item:
                logging.info('set without select converted')
                item = item.strip()
                querystring = re.sub(r'set','var',item)
                proc = querystring
                query_resp.append(proc)

        proc = conf_map['catch']
        query_resp.append(proc)
        with open(op_file, 'w') as f:
            for item in query_resp:
                f.write("%s\n\n\n" % item) 
        logging.info('Completed  file --'+file_name)
        logging.info('--------------------------------')
        
        print('completed')
    except Exception as Er:
        logging.info('Error --'+str(Er))
        print('Exception')
        
# if __name__ == '__main__':
#      src_path = "D:\\Projects\\FMI\\Proc\\"
#      inp_list = os.listdir(src_path)
#      for files in inp_list:
#          file = src_path+files
#          #print(file)
#          proc_converter(file)
#          op_file = file.split('\\')[-1].split('.')[0]+'_converted.txt'
#          op_file = "D:\\Projects\\FMI\\Converted\\" + op_file
#          print(op_file)


                  


               
