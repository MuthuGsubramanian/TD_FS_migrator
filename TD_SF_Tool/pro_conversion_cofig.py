conf_map = {
    'replace procedure': '''CREATE OR REPLACE PROCEDURE {0} ({1})
                         returns string
    			 language javascript
    			 strict
    			 execute as owner
    			 as
    			 $$''',
    'merge into':   'var_sql_merge_base = {0}',
    'as_block':  'AS'+'\n' +" $$"+ ' ' +'try {' ,

    'sf_exe_b' : '''snowflake.execute( {sqlText: var_sql_logical_delete_capture + ";"} ); ''',

    'sf_exe_e' : '''
                    }
                catch (err)
                    {
                    return "Failed: " + err;   
                    }
                    $$;''',
    'try' : 'try {',
    'declareset' : '',
    'set' : ''' var {name1}= snowflake.execute( sqlText: "{query}") ;
		    {name1}.next();
		    var {name}= {name1}.getColumnValue(1);''',
   'delete': ''' var{name} =`{value}`
		snowflake.execute( sqlText: "truncate var{name}" + ";" );
		''',
   'deletewhere': ''' var{name} =`{value}`
		snowflake.execute( sqlText: var{name}" + ";" );
		''',
   'call' : '''var{name}=`{value}`
	       snowflake.execute( sqlText: var{name} + ";");''',
   'merge' : ''' var{name}=`{value}`
		snowflake.execute( sqlText: var{name} + ";" ); ''',
   'update': '''var{name} =`{value}`
		 snowflake.execute( sqlText: var{name} + ";" );''',
   'catch'  : ''' 
		return "Succeeded.";
 
 		}


 	}
	catch (err)
    	{
    	return "Failed: " + err;   
    	}
    	$$
	'''

}