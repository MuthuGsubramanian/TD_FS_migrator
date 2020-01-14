import os
import re
from tkinter import filedialog
from tkinter import *
from procedure_converter import proc_converter
from macro_converter import query_processor
from view_converter import view_processor

if __name__ == '__main__':
    #
    root = Tk()
    root.withdraw()
    src_folder_selected = filedialog.askdirectory(title = 'Choose the Source Directory')
    dest_folder_selected = filedialog.askdirectory(title = 'Choose the destination Directory')

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
