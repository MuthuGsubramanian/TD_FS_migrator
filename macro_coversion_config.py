conf_map = {
    'create macro': '''CREATE OR REPLACE PROCEDURE "{0}"."{1}"()
                        RETURNS VARCHAR(16777216)
                        LANGUAGE JAVASCRIPT
                        STRICT
                        EXECUTE AS OWNER''',
    'merge into':   "var_sql_merge_base = '{0}'",
    'date_add': "dateadd('second', {0}, {1}),",
    'as_block':  'AS'+'\n' +" $$"+ '\n' +'try {' ,
    'sf_exe_m' : '''snowflake.execute( {sqlText: var_sql_merge_base + ";"} ); ''',

    'sf_exe_b' : '''snowflake.execute( {sqlText: var_sql_logical_delete_capture + ";"} ); ''',

    'sf_exe_e' : '''
                    }
                catch (err)
                    {
                    return "Failed: " + err;   
                    }
                    $$;'''
}

