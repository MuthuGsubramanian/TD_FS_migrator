import os
import re
import logging
from tool_conf import *
from tkinter import filedialog
from tkinter import messagebox
from tkinter import *
from procedure_converter import proc_converter
from macro_conv_new import query_processor
from view_converter import view_processor
from r_converter_updated import converter
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
    config = filedialog.askopenfile(title = 'choose the table mapping')
    # config = None
    logging.info(str(dest_folder_selected))
    inp_list = os.listdir(src_folder_selected)
    total_no_of_files = 0
    no_of_macro = 0
    no_of_view = 0
    no_of_procedure = 0

    for files in inp_list:
        total_no_of_files = total_no_of_files+1
        file_name = files.replace('.txt','').replace('.sql','')
        file = src_folder_selected+'/'+files
        rm_space = re.compile(r'\s+')
        with open(file, 'r') as inp:
            op = inp.readlines()
            mac = ''.join(op)
            if re.compile(r'create macro').search(re.sub(rm_space, ' ', mac.lower())):
                query_processor(mac,file_name,dest_folder_selected,config)
                no_of_macro = no_of_macro +1
            elif re.compile(r'replace macro').search(re.sub(rm_space, ' ', mac.lower())):
                query_processor(mac,file_name,dest_folder_selected,config)
                no_of_macro = no_of_macro + 1
            elif file.split('.')[-1]:
                converter(mac,file_name,dest_folder_selected)
            elif re.compile(r'create view').search(re.sub(rm_space, ' ', mac.lower())):
                view_processor(mac,file_name,dest_folder_selected)
                no_of_view = no_of_view + 1
            elif re.compile(r'replace view').search(re.sub(rm_space, ' ', mac.lower())):
                view_processor(mac,file_name,dest_folder_selected)
                no_of_view = no_of_view + 1
            elif re.compile(r'create procedure').search(re.sub(rm_space, ' ', mac.lower())):
                proc_converter(mac,file_name,dest_folder_selected)
                no_of_procedure = no_of_procedure+1
            elif re.compile(r'replace procedure').search(re.sub(rm_space, ' ', mac.lower())):
                proc_converter(mac,file_name,dest_folder_selected)
                no_of_procedure = no_of_procedure+1
            else:
                print('False')
    logging.info('Total no of macros processed: ' + str(no_of_macro))
    logging.info('Total no of views processed: ' + str(no_of_view))
    logging.info('Total no of procedures processed: ' + str(no_of_procedure))
    logging.info('Total no of fies processed: '+ str(total_no_of_files))
    messagebox.showinfo("Completed", "Conversion is completed, Kindly verify the destination folder for results")
    return 'completed'

if __name__ == '__main__':
    i = converter_engine()
    print(i)
