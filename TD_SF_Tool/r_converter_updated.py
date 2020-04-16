# -*- coding: utf-8 -*-
"""
Created on Tue Feb 18 17:20:42 2020

@author: 45042
"""
import os
import re
import logging
import pandas as pd
from tool_conf import *
from tkinter import filedialog
from tkinter import *
from sql_constructor import mapping

logging.basicConfig(
    filename= log_file,
    filemode='w+',
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

def converter(content,file,op_path):
    ''' get files as input
    and returns the converted r script '''
    #with open('D:\Projects\FMI\Teradata Actual Procedure1.txt', 'r') as inp:
    content = re.sub('hive_query','snowflake_query',content)
    content = re.sub('localtime','local_time',content)
    content = re.sub('localtime','local_time',content)

    value = re.findall('date_sub.*?\)',content)
    

    for each in value:
        s = each.split('(')[1].split(',')
        #dateadd(day,-7,'",l_date,"')
        modified = 'dateadd(day,'+'-'+s[3][:-1]+',' + s[0]+','+s[1]+','+s[2]+')'
        logging.info('converted dateadd function')
        #date_sub('",l_date,"', 7)
        print(each)
        #print(modified)
        content = content.replace(each,modified)#)
        logging.info('content modified')
    #date_sub('",l_date,"', 7)
    #date_sub('",l_date,"', 7)
   
    data = pd.read_excel('table_mapping.xlsx')
    pattern = re.compile('from.*?\.')
    createview = []
    for match in pattern.finditer(content):
        createview.append(match.group(0).split(' ')[1])
        database = match.group(0).split(' ')[1]
        database = database[:-1]
        sf = mapping(database,None)
        print(database)
        if database in data['DatabaseName'].values:
            print('yes')
            print(data.head())
            database_subjectArea = data.loc[data['database name']==database,['Database by Subject Area']].reset_index(drop=True)
            print('done')
            database_subjectArea = database_subjectArea.loc[0,'Database by Subject Area']
            print('done2')
            database_subjectArea = database_subjectArea + '.'
            content = content.replace(database+'.',database_subjectArea)
        
    op_file = file.split('\\')[-1].split('.')
    del (op_file[-1])
    print('Delete Successfull')
    op_file_name = ''.join(op_file).strip() + '_converted.R'
    op_file = op_path + op_file_name
    with open(op_file, 'w') as f:   
         
         f.write("%s\n\n\n" % content) 
# if __name__ == '__main__':
#
#     src_path = "‪C:\\Users\\45444\\Downloads\\files\\files\\Extracted_R\\"
#     op_path = "C:\\Users\\45444\\PycharmProjects\\T2S\\TD_FS_migrator\\files\\converted"
#     inp_list = os.listdir(src_path)
#     logging.info('Initiated conversion for R')
#     for files in inp_list:
#         file = src_path+files
#         converter(file, op_path)
