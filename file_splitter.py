import re
path = r'C:\Users\45444\PycharmProjects\TD_FS_migrator\TeradataActualPOCProc_macro.txt'
rm_space = re.compile(r'\s+')
with open(path, 'r') as inp:
    d = inp.readlines()
    splitted = []
    for index,cont in enumerate(d):
        if cont.startswith('-----'):
            file_type = re.sub(rm_space, ' ', d[index+3])
            # print(file_type)
            if file_type.startswith('create macro') or file_type.startswith('replace macro'):
                splitted.append(index)
    ind_1, ind_2 = splitted[::2], splitted[1::2]
    ind_dict = dict(zip(ind_1,ind_2))
    for k,v in ind_dict.items():
        root = "extracted\\"
        file_name= re.sub(rm_space, ' ', d[k:v][3]).strip()+'.txt'
        content = d[k:v]
        with open(file_name, 'w') as f:
            for item in content:
                f.write("%s\n" % item)

