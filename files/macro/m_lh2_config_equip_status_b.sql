create  macro edw_stage_016.m_lh2_config_equip_status_b
as
(
merge into edw_target.lh2_config_equip_status_b as tgt 
using 
( 
select  
	stg.orig_src_id ,  
	stg.site_code ,  
	stg.statusenumtypeid ,  
	stg.statusindex ,  
	stg.timecategoryenumtypeid ,  
	stg.timecategoryindex ,  
	stg.dw_logical_delete_flag ,  
	stg.dw_modify_ts ,  
	stg.dw_load_ts   
from   edw_stage_016.lh2_config_equip_status as stg 
) as stg 
on ( stg.site_code = tgt.site_code and 
stg.statusenumtypeid = tgt.statusenumtypeid and 
stg.statusindex = tgt.statusindex)

when matched then 
update set 
	orig_src_id =  stg.orig_src_id , 
	timecategoryenumtypeid =  stg.timecategoryenumtypeid , 
	timecategoryindex =  stg.timecategoryindex , 
	dw_logical_delete_flag =  stg.dw_logical_delete_flag , 
	dw_modify_ts =  stg.dw_modify_ts 

when not matched then 
insert values (
	stg.orig_src_id , 
	stg.site_code , 
	stg.statusenumtypeid , 
	stg.statusindex , 
	stg.timecategoryenumtypeid , 
	stg.timecategoryindex , 
	stg.dw_logical_delete_flag , 
	stg.dw_modify_ts , 
	stg.dw_load_ts);

--  step 2 ( for  deleted  records)
--  it  is based on incrementaly  loaded records in  stage
--  includes all the change type   ---   d


update edw_target.lh2_config_equip_status_b as tgt
set dw_logical_delete_flag ='Y',
    dw_modify_ts =  (select max(dw_modify_ts)  from edw_stage_016 .lh2_config_equip_status)
where 
tgt.dw_logical_delete_flag <>  'Y' 
and not exists 
      (
      select 'X'
      from edw_stage_016.lh2_config_equip_status as stg
      where  stg.site_code = tgt.site_code and  
      stg.statusenumtypeid = tgt.statusenumtypeid and 
      stg.statusindex = tgt.statusindex
      )
and  exists 
      (
      select 'X'
      from edw_stage_016.lh2_config_equip_status as stg
      where  stg.site_code = tgt.site_code 
      );

);