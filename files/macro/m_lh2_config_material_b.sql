create  macro edw_stage_016.m_lh2_config_material_b
as
(
merge into edw_target.lh2_config_material_b as tgt 
using 
( 
select  
	stg.orig_src_id ,  
	stg.site_code ,  
	stg.materialenumtypeid ,  
	stg.materialidx ,  
	stg.materialgroupenumtypeid ,  
	stg.materialgroupidx ,  
	stg.materialdensity ,  
	stg.src_change_id ,  
	stg.dw_logical_delete_flag ,  
	stg.dw_modify_ts ,  
	stg.dw_load_ts   
from   edw_stage_016.lh2_config_material as stg     
where stg.change_type in ('I','U')
) as stg 
on ( stg.site_code = tgt.site_code and 
stg.materialenumtypeid = tgt.materialenumtypeid and 
stg.materialidx = tgt.materialidx)

when matched then 
update set 
	orig_src_id =  stg.orig_src_id , 
	materialgroupenumtypeid =  stg.materialgroupenumtypeid , 
	materialgroupidx =  stg.materialgroupidx , 
	materialdensity =  stg.materialdensity , 
	src_change_id =  stg.src_change_id , 
	dw_logical_delete_flag =  stg.dw_logical_delete_flag , 
	dw_modify_ts =  stg.dw_modify_ts 

when not matched then 
insert values (
	stg.orig_src_id , 
	stg.site_code , 
	stg.materialenumtypeid , 
	stg.materialidx , 
	stg.materialgroupenumtypeid , 
	stg.materialgroupidx , 
	stg.materialdensity , 
	stg.src_change_id , 
	stg.dw_logical_delete_flag , 
	stg.dw_modify_ts , 
	stg.dw_load_ts);
--  step 2 ( for  deleted  records)
--  it  is based on incrementaly  loaded records in  stage
--  includes all the change type   ---   d
update  edw_target.lh2_config_material_b  
from edw_stage_016 .lh2_config_material stg 
set 
dw_logical_delete_flag = stg.dw_logical_delete_flag , 
dw_modify_ts = stg. dw_modify_ts 
where 
stg.site_code = edw_target.lh2_config_material_b.site_code and  
stg.materialenumtypeid = edw_target.lh2_config_material_b.materialenumtypeid and  
stg.materialidx = edw_target.lh2_config_material_b.materialidx and  stg.change_type ='D'   ;
);