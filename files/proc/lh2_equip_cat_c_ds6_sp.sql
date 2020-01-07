replace  procedure edw_etl.lh2_equip_cat_c_ds6_sp ( start_shift_date DATE,end_shift_date DATE, filter_cliid VARCHAR(100), extract_mode Varchar(10))
begin

'''/  ****************************************************************************************************************************************************
* PK of collection table: cliid , shiftindex, eqmtid,hos
* PURPOSE : Load the lh2_equip_cat_c collection table from  lh2_equip_cat_ds6_etl view based on dw_modify_ts of base table edw_target.lh2_shift_state_b
*
* Usage :
call edw_etl.lh2_equip_cat_c_ds6_sp ('1900-01-01' ,'9999-12-31' ,'16280,18176,18197,18404,19259,22362,32462,32578,32800,36616,36623,42468,1554590','FULL' );
call edw_etl.lh2_equip_cat_c_ds6_sp (null,null,null ,'Delta' )
 Procedure Steps:
 Initial: Set the procedure run time 
    Get the max of modify time from base table
            check  job execution mode and define the filter 
	Step 1a: Before actual table we have to bulit the  stage lh2_equip_state_oper_equip_cat
	Step 1b: insert into  lh2_equip_state_oper_equip_cat for better performance 
Step1: Delete the stage table
Step2: insert the data set into stage table based on the filter condition
Step3: merge into target table
Step4: Capture the deleted rows
Step5: Cleanup the target table 
Step6: update the di job contorl entry ts base table
* CREATE/CHANGE LOG : 
* DATE                     MOD BY                               GCC                           DESC
*-------------------------------------   
*    2019-03-08       Kali D         Initial version
*   2019-08-30      Sabarish S      	 Incremental load enhancement  (proc_var_filter_list  - logic change for delta.)
															Etl view is being filered with variable proc_var_filter_list which filters shiftid/shiftindex and site code
															Previous filter was made only with the driving table;
															If non-driving table gets refreshed there wont be any impact in this stored procedure 
															so driving table will be up to date whereas non-driving table will not get latest data
															New filter includes all tables in it so that driving or non-driving table which ever get refreshed, there will be an impact in this stored procedure
*    *****************************************************************************************************************************************************/  

declare proc_var_dw_modify_ts varchar(19) ;
declare proc_var_ds6_base_max_dw_modify_ts varchar(19);
declare proc_var_di_last_run_ts varchar(19);
declare proc_var_di_last_run_ts_no_look_back timestamp(0);
declare proc_var_filter_list varchar(8000);


set proc_var_dw_modify_ts = (select cast(current_timestamp(0) as varchar (19)));
set proc_var_di_last_run_ts_no_look_back = (select extract_high_ts    from edw_target.di_job_control_entry_ts_base where job_name ='job_lh2_equip_cat_c_ds6_sp');
set proc_var_di_last_run_ts = (select extract_high_ts - cast( 24*60* lookback_days_dec as interval minute(4))   from edw_target.di_job_control_entry_ts_base where job_name ='job_lh2_equip_cat_c_ds6_sp');
set proc_var_ds6_base_max_dw_modify_ts =  (select max(dw_modify_ts) from edw_target.lh2_shift_state_b ); 


if extract_mode ='FULL'
   then 
    set proc_var_filter_list = ' shiftdate between '''|| start_shift_date ||''' and '''|| end_shift_date||''' and  cliid in  ( select cast(token as int)   from table (strtok_split_to_table(1,'''||filter_cliid||''', '','' ) returns (outkey integer,tokennum integer,token varchar(20) character set unicode)) as cliid_list  )' ;
   else 
                   if  (select cast( proc_var_ds6_base_max_dw_modify_ts as timestamp) base_tbl_load_time ) > (select cast( proc_var_di_last_run_ts as timestamp)  as di_load_time)
                            then
                            set proc_var_ds6_base_max_dw_modify_ts = proc_var_di_last_run_ts ;
                        end if;
   -- Sabarish S 2019-08-30 added as part of incremental load enhancement  
   set proc_var_filter_list = ' ( shiftid, site_code) in   
(select shiftid,site_code from edw_target.lh2_shift_aux_b  where dw_modify_ts >= timestamp ''' || proc_var_ds6_base_max_dw_modify_ts  ||''' union
select shiftid,site_code from edw_target.lh2_shift_state_b  where dw_modify_ts >= timestamp ''' || proc_var_ds6_base_max_dw_modify_ts  ||''' union 
select shiftid,site_code from edw_target.lh2_shift_reason_b  where dw_modify_ts >= timestamp ''' || proc_var_ds6_base_max_dw_modify_ts  ||''' union 
select shiftid,site_code from edw_target.lh2_shift_eqmt_b  where dw_modify_ts >= timestamp ''' || proc_var_ds6_base_max_dw_modify_ts  ||''' union
select shiftid,site_code from edw_target.lh2_shift_oper_b  where dw_modify_ts >= timestamp ''' || proc_var_ds6_base_max_dw_modify_ts  ||''' ) ' ;
   end if;


--Step1a: Delete the stage lh2_equip_state_oper table

delete edw_stage.lh2_equip_state_oper_equip_cat;

--Step 1b: insert the data set into stage table based on the filter condition
call dbc.sysexecsql (' insert into edw_stage.lh2_equip_state_oper_equip_cat ( site_code,'  
  || 'shiftindex, ' 
  || 'shiftid, ' 
  || 'shiftdate, ' 
  || 'eqmt_id, ' 
  || 'cliid, ' 
  || 'eqmt_name, ' 
  || 'eqmt_enum_type, ' 
  || 'eqmt_enum_type_code, ' 
  || 'enum_timecat_code, ' 
  || 'enum_timecat_descr, ' 
  || 'status_event_start_time, ' 
  || 'status_event_end_time, ' 
  || 'status_event_duration_sec, ' 
  || 'fieldreason, ' 
  || 'fieldreasonrec, ' 
  || 'state_enum_status_code, ' 
  || 'fieldcomment, ' 
  || 'oper_login_num, ' 
  || 'oper_logout_num, ' 
  || 'oper_id, ' 
  || 'oper_name, ' 
  || 'oper_start_time, ' 
  || 'oper_end_time, ' 
  || 'starttime_time, ' 
  || 'starttime_num, ' 
  || 'endtime_time, ' 
  || 'endtime_num, ' 
  || 'oper_available_time, ' 
  || 'oper_available_time_sec, ' 
  || 'tmcat01, ' 
  || 'tmcat02, ' 
  || 'tmcat03, ' 
  || 'tmcat04, ' 
  || 'tmcat05, ' 
  || 'tmcat06, ' 
  || 'tmcat07, ' 
  || 'tmcat08, ' 
  || 'tmcat09, ' 
  || 'tmcat10, ' 
  || 'tmcat11, ' 
  || 'tmcat12, ' 
  || 'tmcat13, ' 
  || 'tmcat14, ' 
  || 'tmcat15, ' 
  || 'tmcat16, ' 
  || 'tmcat17, ' 
  || 'tmcat18, ' 
  || ' tmcat19 )'
 || 'select  site_code,'  
  || ' shiftindex, ' 
  || ' shiftid, ' 
  || ' shiftdate, ' 
  || ' eqmt_id, ' 
  || ' cliid, ' 
  || ' eqmt_name, ' 
  || ' eqmt_enum_type, ' 
  || ' eqmt_enum_type_code, ' 
  || ' enum_timecat_code, ' 
  || ' enum_timecat_descr, ' 
  || ' status_event_start_time, ' 
  || ' status_event_end_time, ' 
  || ' status_event_duration_sec, ' 
  || ' fieldreason, ' 
  || ' fieldreasonrec, ' 
  || ' state_enum_status_code, ' 
  || ' fieldcomment, ' 
  || ' oper_login_num, ' 
  || ' oper_logout_num, ' 
  || ' oper_id, ' 
  || ' oper_name, ' 
  || ' oper_start_time, ' 
  || ' oper_end_time, ' 
  || ' starttime_time, ' 
  || ' starttime_num, ' 
  || ' endtime_time, ' 
  || ' endtime_num, ' 
  || ' oper_available_time, ' 
  || ' oper_available_time_sec, ' 
  || ' tmcat01, ' 
  || ' tmcat02, ' 
  || ' tmcat03, ' 
  || ' tmcat04, ' 
  || ' tmcat05, ' 
  || ' tmcat06, ' 
  || ' tmcat07, ' 
  || ' tmcat08, ' 
  || ' tmcat09, ' 
  || ' tmcat10, ' 
  || ' tmcat11, ' 
  || ' tmcat12, ' 
  || ' tmcat13, ' 
  || ' tmcat14, ' 
  || ' tmcat15, ' 
  || ' tmcat16, ' 
  || ' tmcat17, ' 
  || ' tmcat18, ' 
  || ' tmcat19 from edw_etl_view.lh2_equip_state_oper_etl   '
 ||'  where  '|| proc_var_filter_list ||';'
);


--Step1: Delete the stage table

delete edw_stage.lh2_equip_cat_ds6;

--Step2: insert the data set into stage table based on the filter condition
call dbc.sysexecsql (' insert into edw_stage.lh2_equip_cat_ds6 ( shiftindex,'  
  || 'shiftdate, ' 
  || 'site_code, ' 
  || 'cliid, ' 
  || 'ddbkey, ' 
  || 'eqmtid, ' 
  || 'eqmtid_orig, ' 
  || 'hos, ' 
  || 'intvl, ' 
  || 'tmcat00, ' 
  || 'tmcat01, ' 
  || 'tmcat02, ' 
  || 'tmcat03, ' 
  || 'tmcat04, ' 
  || 'tmcat05, ' 
  || 'tmcat06, ' 
  || 'tmcat07, ' 
  || 'tmcat08, ' 
  || 'tmcat09, ' 
  || 'tmcat10, ' 
  || 'tmcat11, ' 
  || 'tmcat12, ' 
  || 'tmcat13, ' 
  || 'tmcat14, ' 
  || 'tmcat15, ' 
  || 'tmcat16, ' 
  || 'tmcat17, ' 
  || 'tmcat18, ' 
  || 'tmcat19, ' 
  || 'unit, ' 
  || 'system_version, ' 
  || 'dw_logical_delete_flag, ' 
  || 'dw_modify_ts, ' 
  || ' dw_load_ts )'
 || 'select  shiftindex,'  
  || ' shiftdate, ' 
  || ' site_code, ' 
  || ' cliid, ' 
  || ' ddbkey, ' 
  || ' eqmtid, ' 
  || ' eqmtid_orig, ' 
  || ' hos, ' 
  || ' intvl, ' 
  || ' tmcat00, ' 
  || ' tmcat01, ' 
  || ' tmcat02, ' 
  || ' tmcat03, ' 
  || ' tmcat04, ' 
  || ' tmcat05, ' 
  || ' tmcat06, ' 
  || ' tmcat07, ' 
  || ' tmcat08, ' 
  || ' tmcat09, ' 
  || ' tmcat10, ' 
  || ' tmcat11, ' 
  || ' tmcat12, ' 
  || ' tmcat13, ' 
  || ' tmcat14, ' 
  || ' tmcat15, ' 
  || ' tmcat16, ' 
  || ' tmcat17, ' 
  || ' tmcat18, ' 
  || ' tmcat19, ' 
  || ' unit, ' 
   ||'  ''Dispatch 6'' as system_version,'
 ||'  ''N'' as dw_logical_delete_flag,'
||'  timestamp ''' || proc_var_dw_modify_ts  ||'''  as dw_modify_ts,'
||'  timestamp ''' || proc_var_dw_modify_ts  ||'''  as dw_load_ts from edw_etl_view.lh2_equip_cat_ds6_etl   '
||'  where  '|| proc_var_filter_list ||';'
);

--Step3: merge into target table
merge into edw_target.lh2_equip_cat_c as tgt 
using 
( 
select  
shiftindex ,  
 shiftdate ,  
 site_code ,  
 cliid ,  
 ddbkey ,  
 eqmtid ,  
 eqmtid_orig ,  
 hos ,  
 intvl ,  
 tmcat00 ,  
 tmcat01 ,  
 tmcat02 ,  
 tmcat03 ,  
 tmcat04 ,  
 tmcat05 ,  
 tmcat06 ,  
 tmcat07 ,  
 tmcat08 ,  
 tmcat09 ,  
 tmcat10 ,  
 tmcat11 ,  
 tmcat12 ,  
 tmcat13 ,  
 tmcat14 ,  
 tmcat15 ,  
 tmcat16 ,  
 tmcat17 ,  
 tmcat18 ,  
 tmcat19 ,  
 unit ,  
 system_version ,  
 dw_logical_delete_flag ,  
 dw_modify_ts ,  
 dw_load_ts   
from  edw_stage.lh2_equip_cat_ds6 
) as stg 
ON ( stg.shiftindex = tgt.shiftindex and 
stg.cliid = tgt.cliid and 
stg.ddbkey = tgt.ddbkey and 
stg.eqmtid = tgt.eqmtid and 
stg.hos = tgt.hos
)

WHEN MATCHED THEN 
UPDATE SET 
shiftdate =  stg.shiftdate , 
 site_code =  stg.site_code , 
 eqmtid =  stg.eqmtid , 
 eqmtid_orig =  stg.eqmtid_orig , 
 hos =  stg.hos , 
 intvl =  stg.intvl , 
 tmcat00 =  stg.tmcat00 , 
 tmcat01 =  stg.tmcat01 , 
 tmcat02 =  stg.tmcat02 , 
 tmcat03 =  stg.tmcat03 , 
 tmcat04 =  stg.tmcat04 , 
 tmcat05 =  stg.tmcat05 , 
 tmcat06 =  stg.tmcat06 , 
 tmcat07 =  stg.tmcat07 , 
 tmcat08 =  stg.tmcat08 , 
 tmcat09 =  stg.tmcat09 , 
 tmcat10 =  stg.tmcat10 , 
 tmcat11 =  stg.tmcat11 , 
 tmcat12 =  stg.tmcat12 , 
 tmcat13 =  stg.tmcat13 , 
 tmcat14 =  stg.tmcat14 , 
 tmcat15 =  stg.tmcat15 , 
 tmcat16 =  stg.tmcat16 , 
 tmcat17 =  stg.tmcat17 , 
 tmcat18 =  stg.tmcat18 , 
 tmcat19 =  stg.tmcat19 , 
 unit =  stg.unit , 
 system_version =  stg.system_version , 
 dw_logical_delete_flag =  stg.dw_logical_delete_flag , 
 dw_modify_ts =  stg.dw_modify_ts 

WHEN NOT MATCHED THEN 
INSERT VALUES (
 stg.shiftindex , 
  stg.shiftdate , 
  stg.site_code , 
  stg.cliid , 
  stg.ddbkey , 
  stg.eqmtid , 
  stg.eqmtid_orig , 
  stg.hos , 
  stg.intvl , 
  stg.tmcat00 , 
  stg.tmcat01 , 
  stg.tmcat02 , 
  stg.tmcat03 , 
  stg.tmcat04 , 
  stg.tmcat05 , 
  stg.tmcat06 , 
  stg.tmcat07 , 
  stg.tmcat08 , 
  stg.tmcat09 , 
  stg.tmcat10 , 
  stg.tmcat11 , 
  stg.tmcat12 , 
  stg.tmcat13 , 
  stg.tmcat14 , 
  stg.tmcat15 , 
  stg.tmcat16 , 
  stg.tmcat17 , 
  stg.tmcat18 , 
  stg.tmcat19 , 
  stg.unit , 
  stg.system_version , 
  stg.dw_logical_delete_flag , 
  stg.dw_modify_ts , 
  stg.dw_load_ts);

--Step4: Capture the deleted rows
 
update edw_target.lh2_equip_cat_c  as tgt 
set dw_logical_delete_flag = 'Y' ,
       dw_modify_ts = cast(proc_var_dw_modify_ts as timestamp(0))
where 
tgt.dw_logical_delete_flag <>'Y'
and not exists
      (select 1
      from edw_stage.lh2_equip_cat_ds6 as stg 
		where stg.shiftindex = tgt.shiftindex 
		and stg.cliid = tgt.cliid 
		and stg.ddbkey = tgt.ddbkey  
		and stg.eqmtid = tgt.eqmtid 
		and stg.hos = tgt.hos
         )  
and  exists
     (
     select 'X'
     from edw_stage.lh2_equip_cat_ds6  stg1 
     where  stg1.shiftindex = tgt.shiftindex
     and stg1.cliid = tgt.cliid
     )   ; 

--Step5: Cleanup the target table 
delete   edw_target.lh2_equip_cat_c  where dw_logical_delete_flag = 'Y' and dw_modify_ts <= ( cast(proc_var_dw_modify_ts as timestamp(0)) - interval '3' day );

--Step6: update the di job contorl entry ts base table 
update edw_target.di_job_control_entry_ts_base 
set dw_load_ts =  cast(proc_var_dw_modify_ts as timestamp(0)),
extract_low_ts =  proc_var_di_last_run_ts_no_look_back  ,
extract_high_ts =  coalesce ( (select max(dw_modify_ts) from edw_stage.lh2_equip_cat_ds6), proc_var_di_last_run_ts_no_look_back )
where job_name ='job_lh2_equip_cat_c_ds6_sp';


end;
