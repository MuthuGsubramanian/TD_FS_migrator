import itertools as it
import re
filename= r'C:\Users\45444\PycharmProjects\TD_FS_migrator\TeradataActualPOCProc_macro.txt'
rm_space = re.compile(r'\s+')
with open(filename,'r') as f:
    for key,group in it.groupby(f,lambda line: line.startswith('---')):
        if not key:
            group = list(group)
            macro_check = list(filter(lambda x: 'create macro' in re.sub(rm_space, ' ', x) or 'replace macro' in re.sub(rm_space, ' ', x), group))

            if macro_check:
                file_name = re.sub(rm_space, ' ', macro_check[0]).strip()+'.txt'
                with open('C:\\Users\\45444\\PycharmProjects\\TD_FS_migrator\\files\extracted\\'+file_name, 'w') as f:
                    for item in group:
                        f.write("%s" % item)
