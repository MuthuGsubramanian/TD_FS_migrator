"Request Text"
create   macro edw_stage_016.m_lh2_comm_log_b
as
(
merge into edw_target.lh2_comm_log_b as tgt 
using 
( 
select  
	stg.orig_src_id ,  
	stg.site_code ,  
	stg.comm_log_id ,  
	stg.shiftid, 
	stg.shiftdate ,
	stg.shiftindex ,
	stg.comm_log_timestamp ,  
	stg.messagetype ,  
	coalesce(stg.eqmttype,' ') as eqmttype ,  
	coalesce(stg.eqmptid,' ') as eqmptid ,  
	coalesce(stg.ip,' ') as ip ,  
	stg.seconds ,  
	coalesce(stg.loc,' ') as loc ,  
	coalesce(stg.beacon,' ') as beacon ,  
	stg.received ,  
	stg.sent ,  
	coalesce(stg.messagebody,' ') as messagebody ,  
	stg.src_change_id ,  
	stg.dw_logical_delete_flag ,  
	stg.dw_modify_ts ,  
	stg.dw_load_ts   
from   edw_stage_016.lh2_comm_log as stg  
where stg.change_type in ('I','U')
) as stg 
on ( stg.site_code = tgt.site_code and 
stg.comm_log_id = tgt.comm_log_id  )

when matched then 
update set 
	orig_src_id =  stg.orig_src_id , 
	shiftid =  stg.shiftid , 
	shiftdate = 	stg.shiftdate ,
	shiftindex = stg.shiftindex ,
	comm_log_timestamp =  stg.comm_log_timestamp , 
	messagetype =  stg.messagetype , 
	eqmttype =  stg.eqmttype , 
	eqmptid =  stg.eqmptid , 
	ip =  stg.ip , 
	seconds =  stg.seconds , 
	loc =  stg.loc , 
	beacon =  stg.beacon , 
	received =  stg.received , 
	sent =  stg.sent , 
	messagebody =  stg.messagebody , 
	src_change_id =  stg.src_change_id , 
	dw_logical_delete_flag =  stg.dw_logical_delete_flag , 
	dw_modify_ts =  stg.dw_modify_ts 

when not matched then 
insert values (
	stg.orig_src_id , 
	stg.site_code , 
	stg.comm_log_id , 
	stg.shiftid , 
	stg.shiftdate , 
	stg.shiftindex , 
	stg.comm_log_timestamp , 
	stg.messagetype , 
	stg.eqmttype , 
	stg.eqmptid , 
	stg.ip , 
	stg.seconds , 
	stg.loc , 
	stg.beacon , 
	stg.received , 
	stg.sent , 
	stg.messagebody , 
	stg.src_change_id , 
	stg.dw_logical_delete_flag , 
	stg.dw_modify_ts , 
	stg.dw_load_ts);
--  step 2 ( for  deleted  records)
--  it  is based on incrementaly  loaded records in  stage
--  includes all the change type   ---   d
update  edw_target.lh2_comm_log_b  
from 
( select 
stg.site_code , 
stg.comm_log_id , 
stg.comm_log_timestamp,  
stg.shiftid , 
case  
		when stg.change_type in('D')     then
                      case 
                      when stg.shiftdate<(current_date-170) then 'A' 
                      else 'Y' ---handles  regular delete 
                      end               
    	else 'U'---- for change type not in (i,u,d)
end as dw_logical_delete_flag,
stg.dw_modify_ts ,
stg.change_type
from edw_stage_016.lh2_comm_log stg 
) as stg 
set 
dw_logical_delete_flag = stg.dw_logical_delete_flag , 
dw_modify_ts = stg. dw_modify_ts 
where 
stg.site_code = edw_target.lh2_comm_log_b.site_code and  
stg.comm_log_id = edw_target.lh2_comm_log_b.comm_log_id  
and  stg.change_type ='D' ;
);
