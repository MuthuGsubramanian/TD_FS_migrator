conf_map = {
    'create macro': '''CREATE OR REPLACE PROCEDURE "{0}"."{1}"{2}
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER\n\n''',
    'as_block':  'AS'+'\n' +"$$"+ '\n' +'try {' ,
    'merge_into':   "var_sql_merge_base = `{0})`",
    'insert_into':   "var_sql_insert_base = `{0})`",
    'update':   "{0} = `{1})`",
    'no_into':   "var_sql_base = `{0}`",
    'select': "var_sql_select_base = `{0})`",
    'date_add': "dateadd('second', {0}, {1})",
    'sf_exe_m' : '''\n\nsnowflake.execute( {0} ); ''',
    'sf_exe_update' : '''\n\nsnowflake.execute( {0} ); ''',
    'sf_exe_no' : '''\nsnowflake.execute( {sqlText: var_sql_base } ); ''',
    'sf_exe_delete' : '''snowflake.execute( {sqlText: var_sql_logical_delete_capture } ); ''',
    'sf_exe_e' : ''' }\ncatch (err)
     {
     return "Failed: " + err;   
     }
     $$;'''
}

