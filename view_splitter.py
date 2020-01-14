import re
filename= r'C:\Users\45444\PycharmProjects\TD_FS_migrator\TeradataActualPOCTables_Views.txt'
op = []
with open(filename,'r') as f:
    splitted = f.read().split('--------------------------------------------------------------------------------')
    # for i in splitted:
    #     op.append(i)
    for ind,item in enumerate(splitted):
        with open('C:\\Users\\45444\\PycharmProjects\\TD_FS_migrator\\files\\views\\'+'view_'+str(ind)+'.txt', 'w') as p:
           p.write("%s" % item)
    print(op)