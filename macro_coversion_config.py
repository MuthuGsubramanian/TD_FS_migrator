from change_log import op_log
conf_map = {
    'create macro': '''CREATE OR REPLACE PROCEDURE "{0}"."{1}"()\nRETURNS VARCHAR(16777216)\nLANGUAGE JAVASCRIPT\nSTRICT\nEXECUTE AS OWNER''',
    'replace macro': '''CREATE OR REPLACE PROCEDURE "{0}"."{1}"()\nRETURNS VARCHAR(16777216)\nLANGUAGE JAVASCRIPT\nSTRICT\nEXECUTE AS OWNER''',
    'merge into':   "var_sql_merge_base = `{0})`",
    'date_add': "dateadd('second', {0}, {1})",
    'as_block':  'AS'+'\n' +" $$"+ '\n' +'try {' ,
    'sf_exe_m' : '''snowflake.execute( {sqlText: var_sql_merge_base + ";"} ); ''',

    'sf_exe_b' : '''snowflake.execute( {sqlText: var_sql_logical_delete_capture + ";"} ); ''',

    'sf_exe_e' : ''' }\ncatch (err)
     {
     return "Failed: " + err;   
     }
     $$;'''
}

