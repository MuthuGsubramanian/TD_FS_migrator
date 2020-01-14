


with open(r'C:\Users\45444\PycharmProjects\TD_FS\playground\files\lh2_equip_cat_c_ds6_sp.txt', 'r') as inp:
    op = inp.readlines()
#     mac = ''.join(op)
#     replace = op.split('\n', 1)[0]
# block = ''.join(mac.split(');')).split(';')

def replace_block():
    rep_text = ''.join(op).split('\n', 1)[0]