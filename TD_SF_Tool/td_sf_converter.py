import os
import re
import logging
from tool_conf import *
from tkinter import filedialog
from tkinter import *
from procedure_converter import proc_converter
from macro_converter import query_processor
from view_converter import view_processor

logging.basicConfig(
    filename= log_file,
    filemode='w+',
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

def converter_engine():
    root = Tk()
    root.withdraw()
    src_folder_selected = filedialog.askdirectory(title = 'Choose the Source Directory')
    logging.info(str(src_folder_selected))
    dest_folder_selected = filedialog.askdirectory(title = 'Choose the destination Directory')
    logging.info(str(dest_folder_selected))
    inp_list = os.listdir(src_folder_selected)
    for files in inp_list:
        file_name = files.replace('.txt','').replace('.sql','')
        file = src_folder_selected+'/'+files
        rm_space = re.compile(r'\s+')
        with open(file, 'r') as inp:
            op = inp.readlines()
            mac = ''.join(op)
            if re.compile(r'create macro').search(re.sub(rm_space, ' ', mac.lower())):
                query_processor(mac,file_name,dest_folder_selected)
            elif re.compile(r'replace macro').search(re.sub(rm_space, ' ', mac.lower())):
                query_processor(mac,file_name,dest_folder_selected)
            elif re.compile(r'create set table').search(re.sub(rm_space, ' ', mac.lower())):
                view_processor(mac,file_name,dest_folder_selected)
            elif re.compile(r'create view').search(re.sub(rm_space, ' ', mac.lower())):
                view_processor(mac,file_name,dest_folder_selected)
            elif re.compile(r'replace view').search(re.sub(rm_space, ' ', mac.lower())):
                view_processor(mac,file_name,dest_folder_selected)
            elif re.compile(r'create procedure').search(re.sub(rm_space, ' ', mac.lower())):
                proc_converter(mac,file_name,dest_folder_selected)
            elif re.compile(r'replace procedure').search(re.sub(rm_space, ' ', mac.lower())):
                proc_converter(mac,file_name,dest_folder_selected)
            else:
                print('False')
    return 'completed'

if __name__ == '__main__':
    i = converter_engine()
    print(i)