import itertools as it
import re
filename= r'C:\Users\45444\PycharmProjects\TD_FS_migrator\TeradataActualPOCTables_Views.txt'
rm_space = re.compile(r'\s+')
with open(filename,'r') as f:
    new_name = None
    for key,group in it.groupby(f,lambda line: line.startswith('-----')):
        if not key:
            group = list(group)
            macro_check = list(filter(lambda x: 'CREATE VIEW' in re.sub(rm_space, ' ', x) or 'CREATE SET' in re.sub(rm_space, ' ', x) or 'replace view' in re.sub(rm_space, ' ', x) or 'create view' in re.sub(rm_space, ' ', x), group))
            ext_file = ''.join(f).split(' ')
            for iname in ext_file:
                if '.' in iname:
                    new_name = iname
            if macro_check:
                file_name = re.sub(rm_space, ' ', new_name).strip()+'.txt'
                with open('C:\\Users\\45444\\PycharmProjects\\TD_FS_migrator\\files\extracted\\'+file_name, 'w') as f:
                    for item in group:
                        f.write("%s" % item)

        else:
            print('True')

    print('completed')
