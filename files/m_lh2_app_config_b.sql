"Request Text"
create  macro edw_stage_016.m_lh2_app_config_b
as
(
merge into edw_target.lh2_app_config_b as tgt 
using 
( 
select  
	stg.orig_src_id ,  
	stg.site_code ,  
	stg.app_config_id ,  
	coalesce(stg.scope,' ') as scope ,  
	coalesce(stg.app_config_key,' ') as app_config_key ,  
	coalesce(stg.app_config_value,' ') as app_config_value ,  
	stg.app_config_timestamp ,  
	stg.recordstatus ,  
	stg.src_change_id ,  
	stg.dw_logical_delete_flag ,  
	stg.dw_modify_ts ,  
	stg.dw_load_ts   
from   edw_stage_016.lh2_app_config as stg     
where stg.change_type in ('I','U')
) as stg 
on ( stg.site_code = tgt.site_code and 
stg.app_config_id = tgt.app_config_id)

when matched then 
update set 
	orig_src_id =  stg.orig_src_id , 
	scope =  stg.scope , 
	app_config_key =  stg.app_config_key , 
	app_config_value =  stg.app_config_value , 
	app_config_timestamp =  stg.app_config_timestamp , 
	recordstatus =  stg.recordstatus , 
	src_change_id =  stg.src_change_id , 
	dw_logical_delete_flag =  stg.dw_logical_delete_flag , 
	dw_modify_ts =  stg.dw_modify_ts 

when not matched then 
insert values (
	stg.orig_src_id , 
	stg.site_code , 
	stg.app_config_id , 
	stg.scope , 
	stg.app_config_key , 
	stg.app_config_value , 
	stg.app_config_timestamp , 
	stg.recordstatus , 
	stg.src_change_id , 
	stg.dw_logical_delete_flag , 
	stg.dw_modify_ts , 
	stg.dw_load_ts);
--  step 2 ( for  deleted  records)
--  it  is based on incrementaly  loaded records in  stage
--  includes all the change type   ---   d
update edw_target.lh2_app_config_b  
from  edw_stage_016 .lh2_app_config stg 
set 
dw_logical_delete_flag = stg.dw_logical_delete_flag , 
dw_modify_ts = stg. dw_modify_ts 
where 
stg.site_code = edw_target.lh2_app_config_b.site_code and  
stg.app_config_id = edw_target.lh2_app_config_b.app_config_id and  
stg.change_type ='D'  ;
);
