from tkinter import *
import os
top = Tk()

def test():
    user_name = user_entry.get()
    user_pwd = pwd.get()

    hive_db = hv_entry.get()
    hive_tbl = hv_tbl_entry.get()
    snowflake_db = sf_entry.get()
    snowflake_schema = sf_sch_entry.get()
    snowflake_tbl = sf_tbl_entry.get()
    print(user_name,sf_entry.get())

top.geometry("780x460")

user,user_entry = Label(top, text="User Name").grid(row=0, column=0),Entry(top)
user_entry.grid(row=0, column=1)
password,pwd = Label(top, text="Password").grid(row=0, column=2),Entry(top,show = '*')
pwd.grid(row=0, column=3)

hv_db,hv_entry = Label(top, text="Hive DB name").place(x=30, y=60), Entry(top)
hv_entry.place(x=120, y=60)
hv_tbl,hv_tbl_entry = Label(top, text="Hive tbl name").place(x=30, y=90), Entry(top)
hv_tbl_entry.place(x=120, y=90)
hv_where,hv_whr_entry = Label(top, text="Hive where cond").place(x=30, y=120), Entry(top)
hv_whr_entry.place(x=140, y=120)
sf_dbname,sf_entry = Label(top, text="Snowflake DB name").place(x=260, y=60), Entry(top)
sf_entry.place(x=380, y=60)
sf_sch,sf_sch_entry = Label(top, text="Snowflake schema").place(x=260, y=90), Entry(top)
sf_sch_entry.place(x=380, y=90)
sf_tbl,sf_tbl_entry = Label(top, text="Snowflake tblname").place(x=260, y=120), Entry(top)
sf_tbl_entry.place(x=380, y=120)
sf_where,sf_whr_entry = Label(top, text="Snowflake where").place(x=260, y=150), Entry(top)
sf_whr_entry.place(x=380, y=150)
sbmitbtn = Button(top, command = test, text="Submit", activebackground="pink", activeforeground="blue").grid(row = 0,column = 4)

top.mainloop()