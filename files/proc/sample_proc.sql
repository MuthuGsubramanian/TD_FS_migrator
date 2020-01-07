
replace procedure  edw_etl.lh2_deltac_sum_c_sp ( start_shift_date DATE,end_shift_date DATE, filter_cliid VARCHAR(100),  extract_mode Varchar(10))
begin

declare proc_var_dw_modify_ts varchar(19) ;
--declare proc_var_rt_max_dw_load_ts varchar(19);
declare proc_var_di_last_run_ts varchar(19);
declare proc_var_di_last_run_ts_no_look_back timestamp(0);
declare proc_var_filter_list varchar(8000);
declare proc_var_filter_list_or_list varchar(32000);


set proc_var_dw_modify_ts = (select cast(current_timestamp(0) as varchar (19)));
set proc_var_di_last_run_ts_no_look_back = (select extract_high_ts    from edw_target.di_job_control_entry_ts_base where job_name ='job_lh2_deltac_sum_c_sp');
set proc_var_di_last_run_ts = ( select extract_high_ts - cast( 24*60* lookback_days_dec as interval minute(4))   from edw_target.di_job_control_entry_ts_base where job_name ='job_lh2_deltac_sum_c_sp');
-- commented because of performance issue set proc_var_rt_max_dw_load_ts =  (select max(dw_modify_ts) from edw_target.lh2_load_c   where shiftindex=shiftindex and dw_logical_delete_flag ='N'  ); 


if extract_mode ='FULL'
   then 
    set proc_var_filter_list = ' shiftdate between '''|| start_shift_date ||''' and '''|| end_shift_date||''' and  cliid in  ( select cast(token as int)   from table (strtok_split_to_table(1,'''||filter_cliid||''', '','' ) returns (outkey integer,tokennum integer,token varchar(20) character set unicode)) as cliid_list  )' ;
--Step 1 a : Delete and Load the Stage table with list of Cliid and Shiftindex we are going to process
			delete edw_stage.lh2_deltac_sum_shift_pre_stage;
			call dbc.sysexecsql (' lock table edw_target.lh2_load_c   for access  '
			||' lock table edw_target.lh2_dump_c   for access  '
			||' lock table edw_target.lh2_location_c   for access  '
			||' lock table edw_target.lh2_oper_list_c   for access  '
			||' insert into edw_stage.lh2_deltac_sum_shift_pre_stage '
			||' (cliid,shiftindex)  '
			||' select	 cliid ,shiftindex' 
			||' from edw_target.lh2_load_c   '
			||' where	shiftindex = shiftindex '
			||' and dw_logical_delete_flag =''N'' '
			||' and ' || proc_var_filter_list  ||' group by 1,2 '
			||'union'
			||' select	 cliid ,shiftindex' 
			||' from edw_target.lh2_dump_c   '
			||' where	shiftindex = shiftindex '
			||' and dw_logical_delete_flag =''N'' '
			||' and ' || proc_var_filter_list  ||' group by 1,2'
			||'union'
			||' select	 cliid ,shiftindex' 
			||' from edw_target.lh2_location_c   '
			||' where	shiftindex = shiftindex '
			||' and dw_logical_delete_flag =''N'' '
			||' and ' || proc_var_filter_list  ||' group by 1,2'
			||'union'						
			||' select	 cliid ,shiftindex' 
			||' from edw_target.lh2_oper_list_c   '
			||' where	shiftindex = shiftindex '
			||' and dw_logical_delete_flag =''N'' '
			||' and ' || proc_var_filter_list  ||' group by 1,2 ;'
			);
			
		else 
  	--                 if  (select cast( proc_var_rt_max_dw_load_ts as timestamp) base_tbl_load_time ) > (select cast( proc_var_di_last_run_ts as timestamp)  as di_load_time)
    --                        then
    --                      set proc_var_rt_max_dw_load_ts = proc_var_di_last_run_ts ;
    --                end if;
--Step 1 a : Delete and Load the Stage table with list of Cliid and Shiftindex we are going to process
			delete edw_stage.lh2_deltac_sum_shift_pre_stage;
			call dbc.sysexecsql (' lock table edw_target.lh2_load_c   for access  '
			||' lock table edw_target.lh2_dump_c   for access  '
			||' lock table edw_target.lh2_location_c   for access  '
			||' lock table edw_target.lh2_oper_list_c   for access  '
			||' insert into edw_stage.lh2_deltac_sum_shift_pre_stage '
			||' (cliid,shiftindex)  '
			||' select	 cliid ,shiftindex' 
			||' from edw_target.lh2_load_c   '
			||' where	shiftindex = shiftindex '
			||' and dw_logical_delete_flag =''N'' '
			--||' and shiftdate >= current_date - 45  group by 1,2 ;'
			||' and dw_modify_ts >=  timestamp ''' || proc_var_di_last_run_ts  ||'''  group by 1,2 '
			||'Union'
			||' select	 cliid ,shiftindex' 
			||' from edw_target.lh2_dump_c   '
			||' where	shiftindex = shiftindex '
			||' and dw_logical_delete_flag =''N'' '
			--||' and shiftdate >= current_date - 45  group by 1,2 ;'
			||' and dw_modify_ts >=  timestamp ''' || proc_var_di_last_run_ts  ||'''  group by 1,2 '
			||'Union'
			||' select	 cliid ,shiftindex' 
			||' from edw_target.lh2_location_c   '
			||' where	shiftindex = shiftindex '
			||' and dw_logical_delete_flag =''N'' '
			--||' and shiftdate >= current_date - 45  group by 1,2 ;'
			||' and dw_modify_ts >=  timestamp ''' || proc_var_di_last_run_ts  ||'''  group by 1,2 '
			||'Union'
			||' select	 cliid ,shiftindex' 
			||' from edw_target.lh2_oper_list_c   '
			||' where	shiftindex = shiftindex '
			||' and dw_logical_delete_flag =''N'' '
			--||' and shiftdate >= current_date - 45  group by 1,2 ;'
			||' and dw_modify_ts >=  timestamp ''' || proc_var_di_last_run_ts  ||'''  group by 1,2 ;'
			);
end if;

 
begin request
--Step 2 : Delete lh_target_load_shift_new table for shiftindex and cliid, for which we are going to rebuilt the table
delete  prod_pddw.lh_target_load_shift_new  where ( shiftindex, cliid) in   ( select shiftindex, cliid from  edw_stage.lh2_deltac_sum_shift_pre_stage group by 1,2 ) ;
  
 --Step 3: Delete lh_target_time_shift_new table for shiftindex and cliid, for which we are going to rebuilt the table
delete  prod_pddw.lh_target_time_shift_new  where ( shiftindex, cliid) in   ( select shiftindex, cliid from  edw_stage.lh2_deltac_sum_shift_pre_stage  group by 1,2 ) ;

end request;
-- Do table inserts into shift tables from base tables via shiftindex 
--  prod_pddw.lh_target_load_shift ETL 
begin request
--Step 4 : Insert lh_target_load_shift_new table for shiftindex and cliid used as parameter for the procedure 
insert into prod_pddw.lh_target_load_shift_new
		(
cliid
,eqmttype#
,shiftindex
,site_code
,fleet
,ops_portal_euip_grp
,ops_portal_equip_grp_id
,size_cu_yd
,secs_per_pass
,idle_time
,spot_time
,load_time
,dump_time_dump
,dump_time_crusher
,dump_time_upper_limiit
,spot_time_upper_limiit
,flat_loaded
,down_loaded
,up_loaded
,flat_empty
,up_empty
,down_empty
,hours_per_day
,min_payload
,target_payload
,target_payload_range
,measure_43_constant
,efh_conv_downhill
,efh_conv_uphill
,payload_comp_target
,lower_tar_prod
,upper_tar_prod
,last_modified_ts
 		)
select 
	  t.cliid
	, tr.eqmttype#
	, t.shiftindex
	, tr.site_code
	, tr.fleet
	, tr.ops_portal_euip_grp
	, tr.ops_portal_equip_grp_id
	, tr.size_cu_yd
	, tr.secs_per_pass
	, tr.idle_time
	, tr.spot_time
	, tr.load_time
	, tr.dump_time_dump
	, tr.dump_time_crusher
	, tr.dump_time_upper_limiit
	, tr.spot_time_upper_limiit
	, tr.flat_loaded
	, tr.down_loaded
	, tr.up_loaded
	, tr.flat_empty
	, tr.up_empty
	, tr.down_empty
	, tr.hours_per_day
	, tr.min_payload
	, tr.target_payload
	, tr.target_payload_range
	, tr.measure_43_constant
	, tr.efh_conv_downhill
	, tr.efh_conv_uphill
	, tr.payload_comp_target
	, tr.lower_tar_prod
	, tr.upper_tar_prod
	, cast(proc_var_dw_modify_ts as timestamp(0)) as  last_modified_ts
from prod_pddw.lh_target_load_base_new tr 
inner join   edw_stage.lh2_deltac_sum_shift_pre_stage  as t 
on tr.cliid = t.cliid
where t.shiftindex >= tr.start_shiftindex 
and t.shiftindex <= tr.end_shiftindex ;



--  prod_pddw.lh_target_time_shift ETL 
--Step 5 : Insert lh_target_time_shift_new table for shiftindex and cliid used as parameter for the procedure 
insert into prod_pddw.lh_target_time_shift_new
		(
cliid
,shovel_eqmttype#
,ops_portal_shvl_grp_id
,truck_eqmttype#
,ops_portal_trck_grp_id
,shiftindex
,site_code
,target_load_time
,last_modified_ts
 		)
select 
		t.cliid
		,tr.shovel_eqmttype#
		,tr.ops_portal_shvl_grp_id
		,tr.truck_eqmttype#
		,tr.ops_portal_trck_grp_id
		,t.shiftindex
		,tr.site_code
		,tr.target_load_time
		, cast(proc_var_dw_modify_ts as timestamp(0)) as  last_modified_ts
from prod_pddw.lh_target_time_base_new tr 
inner join   edw_stage.lh2_deltac_sum_shift_pre_stage  as t 
on tr.cliid = t.cliid
where t.shiftindex >= tr.start_shiftindex 
and t.shiftindex <= tr.end_shiftindex ;



--Step 6: Delete the delta c  stage table

delete edw_stage.lh2_deltac_sum_pre_stage;
delete edw_stage.lh2_deltac_sum;
end request;

--Step 7: insert the data set into stage table based on the filter condition
insert into edw_stage.lh2_deltac_sum_pre_stage ( 
shiftindex,
shiftdate,
site_code,
cliid,
ddbkey,
year#,
month#,
months,
shift,
shift#,
crew#,
crew,
harvhos,
digtype,
dipper,
payload,
idletime,
spottime,
loadtime,
dumpingtime,
crusheridle,
travelempty,
travelloaded,
totalcycle,
loads_disteh,
loads_lift_down,
loads_lift_up,
dumps_dist,
dumps_lift_down,
dumps_lift_up,
hos,
timefull,
timefull_ts,
timeloaded,
excav,
eoper,
truck,
toperid,
toper,
load#,
material,
bench,
unit#,
unit,
dumpname,
load_seq_no,
grade,
extraload,
loadtype,
loadtons,
measuretons_shovel,
over_truck_flag,
under_truck_flag,
dump_flag,
shovel_idle,
crusher_dump_flag,
dump_number,
ldump_hos,
dump_hos,
dump_oper_id,
eliftup,
eliftdown,
distloaded,
fliftup,
fliftdown,
distempty,
emptyhaul,
fullhaul,
load_timearrive,
load_timearrive_ts,
timeload,
timeload_ts,
beginspot,
beginspot_ts,
hangtime,
dump_timearrive,
dump_timearrive_ts,
timedump,
timedump_ts,
timeempty,
timeempty_ts,
material_name,
start_date_ts,
end_date_ts,
system_version,
dw_logical_delete_flag,
dw_modify_ts,
dw_load_ts)
select
 -- We have duplicate in oper_list table
loads.shiftindex,
loads.shiftdate ,
loads.site_code ,
loads.cliid ,
loads.ddbkey ,
extract(year from shift_date.shiftdate) as year#, --- year number
shift_date.month#, ---month number
shift_date.months, ---month
shift_date.shift, ---shift (day - night)
shift_date.shift#, ---shift numer ( 1 - 2)
shift_date.crew#, ---crew number
shift_date.crew, ---crew name
(case
			when shift_date.shift# = 2
			then loads.hos + 12
			else loads.hos
end
) as harvhos, 
null as digtype,
0 as dipper,-- was harded in bteq 4B
case when loads.cliid in (32578, 19259, 1554590, 42468) then
coalesce(nullif(loads.dcstons,0),dumps.measureton)  / (1.10231) 
else coalesce(nullif(loads.dcstons,0),dumps.measureton)  end as payload,  -- tons in truck
zeroifnull(loads.queuetime / 60) as idletime, --- truck idle time at shovel
zeroifnull(loads.spottime / 60) as spottime, --- truck spot time at shovel
zeroifnull(loads.loadingtim / 60) as loadtime, --- time to load truck
zeroifnull(case
	when loc.unit# in (3, 5)
	then (zeroifnull(dumps.dumpingtim) + zeroifnull(dumps.idletime)) / 60.0
	else 0
end) as dumpingtime, --- dumping time at waste and stockpile locations
zeroifnull(case
	when loc.unit# = 4
	then (zeroifnull(dumps.dumpingtim)  + zeroifnull(dumps.idletime)) / 60.0
	else 0
end) as crusheridle, --- dumping time at crushers
zeroifnull(loads.emptyhaul / 60) as travelempty, ---actual travel time empty
zeroifnull(loads.fullhaul / 60) as travelloaded, ---actual travel time loaded
(dumpingtime + crusheridle + zeroifnull(loads.queuetime / 60) + zeroifnull(loads.spottime / 60)  + zeroifnull(loads.loadingtim / 60) + zeroifnull(loads.emptyhaul / 60)  + zeroifnull(loads.fullhaul / 60) )  as totalcycle,
case when loads.cliid in (32578, 19259, 1554590, 42468) then loads.disteh      / (3.28084) else loads.disteh      end  as loads_disteh, 
case when loads.cliid in (32578, 19259, 1554590, 42468) then loads.lift_down / (3.28084) else loads.lift_down end  / 50 as loads_lift_down,
(case when loads.cliid in (32578, 19259, 1554590, 42468) then loads.lift_up / (3.28084) else loads.lift_up end / 50) as loads_lift_up,
(case when dumps.cliid in (32578, 19259, 1554590, 42468) then dumps.dist/ (3.28084) else dumps.dist end) dumps_dist ,
(case when dumps.cliid in (32578, 19259, 1554590, 42468) then dumps.lift_down / (3.28084) else dumps.lift_down end / 50) dumps_lift_down,
(case when dumps.cliid in (32578, 19259, 1554590, 42468) then dumps.lift_up / (3.28084) else dumps.lift_up end / 50)dumps_lift_up,
loads.hos, ---hour of shift
loads.timefull, ---time truck loaded (seconds into  shift)
cast(shift_date.start_date_ts as timestamp(0)) + cast (cast(loads.timefull as number) / 60 as  interval minute(4)) + cast ( (cast(loads.timefull as number)  mod 60 ) as interval second(4,3)) as timefull_ts, --Included on 07/29/2019
((((shift_date.starts + loads.timefull)(int)) / 3600) (format 'zzzz99'))
|| ((((shift_date.starts + loads.timefull)(int)) mod 3600) / 60 (format ':99'))
|| (((shift_date.starts + loads.timefull)(int)) mod 60 (format ':99')) as timeloaded, -- Only adding hours to start time 
loads.excav, --- shovel
loads.eoper,
loads.truck,
truck_oper.operid as toperid,
truck_oper.name as toper,
loads.load as load#, --- material number
mat.name as material, --Load_type table column name = Material
loads.loc as bench, --- shovel bench name
loc.unit#, --- location unit number (3, 4, 5)
loc.unit, --- location unit (waste, 
dumps.loc as dumpname, --- name of dumping location     
0 as load_seq_no,
loads.grade,
loads.extraload,
loads.loadtype,
case when loads.cliid in (32578, 19259, 1554590, 42468) then loads.loadtons_us / (1.10231) else loadtons_us end as loadtons,
0 as measuretons_shovel,
loads.ot as over_truck_flag,
loads.ut as under_truck_flag,
loads.dmp as dump_flag,
((loads.idletime / 60) + (loads.hangtime / 60)) as shovel_idle,
(case
	when loc.unit# = 4
	then 1
	else 0
end) as crusher_dump_flag,
dumps.dumpid as dump_number,
null as ldump_hos,
dumps.hos as dump_hos,
dumps.oper as dump_oper_id,
case when loads.cliid in (32578, 19259, 1554590, 42468) then loads.lift_up/ (3.28084) else loads.lift_up     end as eliftup, ---feet traveled empty up hill
case when loads.cliid in (32578, 19259, 1554590, 42468) then loads.lift_down /(3.28084) else loads.lift_down end  as eliftdown, ---feet traveled empty down hill
case when dumps.cliid in (32578, 19259, 1554590, 42468) then dumps.dist/ (3.28084) else dumps.dist end as distloaded, ---total feet traveled loaded to dumping location
case when dumps.cliid in (32578, 19259, 1554590, 42468) then dumps.lift_up /(3.28084) else dumps.lift_up end as fliftup, ---feet traveled loaded up hill
case when dumps.cliid in (32578, 19259, 1554590, 42468) then dumps.lift_down /(3.28084) else dumps.lift_down end as fliftdown, ---feet traveled loaded down hill
case when loads.cliid in (32578, 19259, 1554590, 42468) then loads.disteh / (3.28084) else loads.disteh      end  as distempty, ---total feet traveled empty to shovel
zeroifnull(loads.emptyhaul) as emptyhaul,
zeroifnull(loads.fullhaul) as fullhaul,
loads.timearrive as load_timearrive,
cast(shift_date.start_date_ts as timestamp(0)) + cast (cast(loads.timearrive as number) / 60 as  interval minute(4)) + cast ( (cast(loads.timearrive as number)  mod 60 ) as interval second(4,3)) as load_timearrive_ts, --Included on 07/29/2019
loads.timeload as timeload,
cast(shift_date.start_date_ts as timestamp(0)) + cast (cast(loads.timeload as number) / 60 as  interval minute(4)) + cast ( (cast(loads.timeload as number)  mod 60 ) as interval second(4,3)) as timeload_ts, --Included on 07/29/2019
zeroifnull(loads.beginspot) as beginspot,
cast(shift_date.start_date_ts as timestamp(0)) + cast (cast(zeroifnull(loads.beginspot) as number) / 60 as  interval minute(4)) + cast ( (cast(zeroifnull(loads.beginspot) as number)  mod 60 ) as interval second(4,3)) as beginspot_ts, --Included on 07/29/2019
zeroifnull(loads.hangtime) as hangtime,
dumps.timearrive as dump_timearrive,
cast(shift_date.start_date_ts as timestamp(0)) + cast (cast(dumps.timearrive as number) / 60 as  interval minute(4)) + cast ( (cast(dumps.timearrive as number)  mod 60 ) as interval second(4,3)) as dump_timearrive_ts, --Included on 07/29/2019
dumps.timedump as timedump,
cast(shift_date.start_date_ts as timestamp(0)) + cast (cast(dumps.timedump as number) / 60 as  interval minute(4)) + cast ( (cast(dumps.timedump as number)  mod 60 ) as interval second(4,3)) as timedump_ts, --Included on 07/29/2019
dumps.timeempty as timeempty,
cast(shift_date.start_date_ts as timestamp(0)) + cast (cast(dumps.timeempty as number) / 60 as  interval minute(4)) + cast ( (cast(dumps.timeempty as number)  mod 60 ) as interval second(4,3)) as timeempty_ts, --Included on 07/29/2019
mat.colloquial_name as  material_name,
shift_date.start_date_ts,
shift_date.end_date_ts ,
loads.system_version,
'N' as dw_logical_delete_flag,
cast(proc_var_dw_modify_ts as timestamp(0)) as dw_modify_ts,
cast(proc_var_dw_modify_ts as timestamp(0)) as dw_load_ts
--select loads.site_code
from
pddw.lh_load as loads
-- filter  condition to restict the number of shift
inner join edw_stage.lh2_deltac_sum_shift_pre_stage stg
on stg.cliid = loads.cliid
and stg.shiftindex = loads.shiftindex

inner join
pddw.lh_shift_date2_view as shift_date
on loads.shiftindex = shift_date.shiftindex
and loads.cliid = shift_date.cliid

inner join
pddw.lh_dump as dumps
on loads.shiftindex = dumps.shiftindex
and loads.dumprec = dumps.ddbkey
and loads.cliid = dumps.cliid

inner join
pddw.lh_location as loc
on dumps.shiftindex = loc.shiftindex
and dumps.cliid = loc.cliid
and dumps.loc = loc.locid


inner join 
pddw.lh_oper_list as truck_oper
on loads.shiftindex = truck_oper.shiftindex
and loads.cliid = truck_oper.cliid
and loads.oper = truck_oper.operid

inner join
app_ops_portal.load_type as mat
on loads.load = mat.num
and loads.cliid = mat.cliid

where
-- load where dump is completed
loads.extraload <>1
and loads.dumprec <> 0
and loads.loadtype <>1
and dumps.loadtype <> 1

;

insert into edw_stage.lh2_deltac_sum
(shiftindex,
shiftdate,
site_code,
cliid,
ddbkey,
year#,
month#,
months,
shift,
shift#,
crew#,
crew,
harvhos,
digtype,
shvtype,
dipper,
trktype,
payload,
idletime,
spottime,
loadtime,
dumpingtime,
crusheridle,
travelempty,
travelloaded,
totalcycle,
calctravempty,
calctravloaded,
hos,
timefull,
timefull_ts,
timeloaded,
excav,
soperid,
soper,
truck,
toperid,
toper,
load#,
material,
bench,
unit#,
unit,
dumpname,
idledelta,
spotdelta,
loaddelta,
dumpdelta,
et_delta,
lt_delta,
delta_c,
toavgdeltac,
tostdevdeltac,
toavgidledelta,
tostdevidledelta,
toavgspotdelta,
tostdevspotdelta,
toavgloaddelta,
tostdevloaddelta,
toavgetdelta,
tostdevetdelta,
toavgltdelta,
tostdevltdelta,
toavgdumpdelta,
tostdevdumpdelta,
vtodeltac3,
load_seq_no,
grade,
extraload,
loadtype,
loadtons,
measuretons_shovel,
over_truck_flag,
under_truck_flag,
dump_flag,
shovel_idle,
crusher_dump_flag,
dump_number,
ldump_hos,
dump_hos,
dump_oper_id,
shovel_eqmttype#,
truck_eqmttype#,
shvl_ops_prtl_equip_group_no#,
trk_ops_prtl_equip_group_no#,
shovel_idledelta,
eliftup,
eliftdown,
distloaded,
fliftup,
fliftdown,
distempty,
emptyhaul,
load_timearrive,
load_timearrive_ts,
timeload,
timeload_ts,
beginspot,
beginspot_ts,
hangtime,
dump_timearrive,
dump_timearrive_ts,
timedump,
timedump_ts,
timeempty,
timeempty_ts,
material_name,
start_date_ts,
end_date_ts,
system_version,
dw_logical_delete_flag,
dw_modify_ts,
dw_load_ts)

select
 -- We have duplicate in oper_list table
loads.shiftindex,
loads.shiftdate ,
loads.site_code ,
loads.cliid ,
loads.ddbkey ,
loads.year#, --- year number
loads.month#, ---month number
loads.months, ---month
loads.shift, ---shift (day - night)
loads.shift#, ---shift numer ( 1 - 2)
loads.crew#, ---crew number
loads.crew, ---crew name
loads.harvhos, 
loads.digtype,
-- calculate in stage 2
equip_list_excav.ops_prtl_equip_group as shvtype, -- shovel group
loads.dipper,-- was harded in bteq 4B
-- calculate in stage 2
equip_list_truck.ops_prtl_equip_group as trktype, -- Truck group
loads.payload,  -- tons in truck
loads.idletime, --- truck idle time at shovel
loads.spottime, --- truck spot time at shovel
loads.loadtime, --- time to load truck
loads.dumpingtime, --- dumping time at waste and stockpile locations
loads.crusheridle, --- dumping time at crushers
loads.travelempty, ---actual travel time empty
loads.travelloaded, ---actual travel time loaded
loads.totalcycle,
--(loads.lift_up / 50) as adv_empty, ---number of benches traveled up empty
--(loads.lift_down / 50) as fav_empty, ---number of benches traveled down empty
--(dumps.lift_up / 50) as adv_loaded, ---number of benches traveled up loaded
--(dumps.lift_down / 50) as fav_loaded, ---number of benches traveled down empty
--(((distempty- ((fav_empty+adv_empty)*500))*60/5280/speed.flat_empty + fav_empty*500*60/5280/speed.down_empty + adv_empty*500*60/5280/speed.up_empty))  	  As CalcTravEmpty,
((loads_disteh  - 
(loads_lift_down 
+ loads_lift_up )*500)*60/case when loads.cliid in (32578, 19259, 22362,1554590, 42468) then 1000 else 5280 end/speed.flat_empty)
+ (loads_lift_down) *500*60/case when loads.cliid in (32578, 19259,22362, 1554590, 42468) then 1000 else 5280 end/speed.down_empty 
+ (loads_lift_up)*500*60/case when loads.cliid in (32578, 19259, 22362,1554590, 42468) then 1000 else 5280 end/speed.up_empty  as calctravempty,
--(((distloaded-((fav_loaded+adv_loaded)*500))*60/5280/speed.flat_loaded + fav_loaded*500*60/5280/speed.down_loaded + adv_loaded*500*60/5280/speed.up_loaded)) 	  As calcTravLoaded,
(dumps_dist 
- ((
    (dumps_lift_down) 
	+ (dumps_lift_up)
	) * 500
	)
	)*60/case when loads.cliid in (32578, 19259, 22362,1554590, 42468) then 1000 else 5280 end /speed.flat_loaded
+ (dumps_lift_down) *500*60/case when loads.cliid in (32578, 19259, 22362,1554590, 42468) then 1000 else 5280 end /speed.down_loaded
+ (dumps_lift_up)*500*60/case when loads.cliid in (32578, 19259, 22362,1554590, 42468) then 1000 else 5280 end /speed.up_loaded as calctravloaded,
loads.hos, ---hour of shift
loads.timefull, ---time truck loaded (seconds into  shift)
loads.timefull_ts, 
loads.timeloaded, -- Only adding hours to start time 
loads.excav, --- shovel
excav_oper.operid as soperid,
excav_oper.name as soper,
loads.truck,
loads.toperid,
loads.toper,
loads.load#, --- material number
loads.material, --Load_type table column name = Material
loads.bench, --- shovel bench name
loads.unit#, --- location unit number (3, 4, 5)
loads.unit, --- location unit (waste, 
loads.dumpname, --- name of dumping location     
loads.idletime - truck_std_time.idle_time as idledelta,
loads.spottime - truck_std_time.spot_time as spotdelta,
loads.loadtime  - excav_std_load_time.target_load_time as loaddelta,
(case
when	loads.unit#=4
-- crusheridle and dumpingtime are calculated above
then crusheridle - truck_std_time.dump_time_crusher
else dumpingtime - truck_std_time.dump_time_dump
end) as dumpdelta,
-- calctravempty and calctravloaded are calculated above
zeroifnull(loads.emptyhaul / 60)  - calctravempty as et_delta,
zeroifnull(loads.fullhaul / 60) - calctravloaded as lt_delta,
(case
when	loads.unit# in(3,5) 
then 
totalcycle 
- (calctravempty + calctravloaded + truck_std_time.idle_time + truck_std_time.spot_time + excav_std_load_time.target_load_time + truck_std_time.dump_time_dump)
else	
totalcycle 
- (calctravempty + calctravloaded + truck_std_time.idle_time + truck_std_time.spot_time + excav_std_load_time.target_load_time + truck_std_time.dump_time_crusher)
end) as delta_c,
avg(delta_c) over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex ) as toavgdeltac,        
stddev_samp(delta_c) over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex ) as tostdevdeltac,
avg(idledelta) over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex ) as toavgidledelta,
stddev_samp(idledelta)over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex )  as tostdevidledelta,
avg(spotdelta) over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex ) as toavgspotdelta,
stddev_samp(spotdelta) over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex ) as tostdevspotdelta,
avg(loaddelta) over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex ) as toavgloaddelta,
stddev_samp(loaddelta) over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex ) as tostdevloaddelta,
avg(et_delta) over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex ) as toavgetdelta,
stddev_samp(et_delta)over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex )  as tostdevetdelta,
avg(lt_delta) over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex ) as toavgltdelta,
stddev_samp(lt_delta) over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex )  as tostdevltdelta,
avg(dumpdelta)over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex )  as toavgdumpdelta,
stddev_samp(dumpdelta)over(partition by loads.cliid,loads.shiftindex,loads.crew#, loads.toperid,loads.truck order by  loads.cliid,loads.shiftindex )  as tostdevdumpdelta,
(case
when	delta_c <= toavgdeltac + (tostdevdeltac*3)	and delta_c >= toavgdeltac - (tostdevdeltac*3)
then 'Y'
else 'N'
end) as vtodeltac3,
loads.load_seq_no,
loads.grade,
loads.extraload,
loads.loadtype,
loads.loadtons,
loads.measuretons_shovel,
loads.over_truck_flag,
loads.under_truck_flag,
loads.dump_flag,
loads.shovel_idle,
loads.crusher_dump_flag,
loads.dump_number,
loads. ldump_hos,
loads.dump_hos,
loads.dump_oper_id,
equip_list_excav.lh_equip_class as shovel_eqmttype#, -- shovel group
equip_list_truck.lh_equip_class as truck_eqmttype#, -- Truck group
equip_list_excav.ops_prtl_equip_group_no  as shvl_ops_prtl_equip_group_no#,
equip_list_truck.ops_prtl_equip_group_no as trk_ops_prtl_equip_group_no#,
shovel_idle - shovel_std_time.idle_time   as shovel_idledelta,
loads.eliftup, ---feet traveled empty up hill
loads.eliftdown, ---feet traveled empty down hill
loads.distloaded, ---total feet traveled loaded to dumping location
loads.fliftup, ---feet traveled loaded up hill
loads.fliftdown, ---feet traveled loaded down hill
loads.distempty, ---total feet traveled empty to shovel
loads.emptyhaul,
loads.load_timearrive,
loads.load_timearrive_ts,
loads.timeload,
loads.timeload_ts,
loads.beginspot,
loads.beginspot_ts,
loads.hangtime,
loads.dump_timearrive,
loads.dump_timearrive_ts,
loads.timedump,
loads.timedump_ts,
loads.timeempty,
loads.timeempty_ts,
loads.material_name,
loads.start_date_ts,
loads.end_date_ts ,
loads.system_version,
loads.dw_logical_delete_flag,
loads.dw_modify_ts,
loads.dw_load_ts
from
edw_stage.lh2_deltac_sum_pre_stage as loads
inner join
 prod_mapping.ops_portal_equipment_list as equip_list_excav
on	loads.excav = equip_list_excav.lh_equip_id
and loads.cliid = equip_list_excav.lh_cliid

inner join
prod_mapping.ops_portal_equipment_list as equip_list_truck
on	loads.truck = equip_list_truck.lh_equip_id
and loads.cliid = equip_list_truck.lh_cliid

inner join pddw.lh_target_time_base_view	as 	excav_std_load_time
on	loads.cliid = excav_std_load_time.cliid
and	loads.shiftindex >= excav_std_load_time.start_shiftindex
and	loads.shiftindex <= excav_std_load_time.end_shiftindex
and	equip_list_excav.ops_prtl_equip_group_no  = excav_std_load_time.ops_portal_shvl_grp_id
and	equip_list_truck.ops_prtl_equip_group_no = excav_std_load_time.ops_portal_trck_grp_id

inner join pddw.lh_target_load_base_view  truck_std_time
on loads.cliid=truck_std_time.cliid and
loads.shiftindex>=truck_std_time.start_shiftindex and
loads.shiftindex<=truck_std_time.end_shiftindex and
equip_list_truck.ops_prtl_equip_group_no = truck_std_time.ops_portal_equip_grp_id

inner join pddw.lh_target_load_base_view  shovel_std_time
on loads.cliid=shovel_std_time.cliid and
loads.shiftindex>=shovel_std_time.start_shiftindex and
loads.shiftindex<=shovel_std_time.end_shiftindex and
equip_list_excav.ops_prtl_equip_group_no = shovel_std_time.ops_portal_equip_grp_id

inner join 
 pddw.lh_oper_list as excav_oper
on loads.shiftindex = excav_oper.shiftindex
and loads.eoper = excav_oper.operid
and loads.cliid = excav_oper.cliid

--rt_deltac_calcs_mor_15min
--Get the average timing for an equipment group\
inner join
    (select
            target_load.cliid,
            target_load.shiftindex,
            site_code,
            ops_portal_euip_grp,
            ops_portal_equip_grp_id,
            target_payload_range ,
            max(last_modified_ts) as last_modified_ts,
            avg(size_cu_yd) as size_cu_yd,
            avg(secs_per_pass) as secs_per_pass,
            avg(idle_time) as idle_time,
            avg(spot_time) as spot_time,
            avg(load_time) as load_time,
            avg(dump_time_dump) as dump_time_dump,
            avg(dump_time_crusher) as dump_time_crusher,
            avg(dump_time_upper_limiit) as dump_time_upper_limiit,
            avg(spot_time_upper_limiit) as spot_time_upper_limiit,
            avg(flat_loaded) as flat_loaded,
            avg(down_loaded) as down_loaded,
            avg(up_loaded) as up_loaded,
            avg(flat_empty) as flat_empty,
            avg(up_empty) as up_empty,
            avg(down_empty) as down_empty,
            avg(hours_per_day) as hours_per_day,
            avg(min_payload) as min_payload,
            avg(target_payload) as target_payload,
            avg(measure_43_constant) as measure_43_constant,
            avg(efh_conv_downhill) as efh_conv_downhill,
            avg(efh_conv_uphill) as efh_conv_uphill,
            avg(payload_comp_target) as payload_comp_target,
            avg(lower_tar_prod) as lower_tar_prod,
            avg(upper_tar_prod) as upper_tar_prod
        from
		prod_pddw.lh_target_load_shift_new target_load
		--- to reduce the number of rows to process
		inner join edw_stage.lh2_deltac_sum_shift_pre_stage stg
		on stg.cliid = target_load.cliid
		and stg.shiftindex = target_load.shiftindex
		group by 1,2,3,4,5,6
    ) as speed
		on loads.cliid = speed.cliid
		and loads.shiftindex = speed.shiftindex
		and equip_list_truck.ops_prtl_equip_group_no = speed. ops_portal_equip_grp_id
; 

--Step 8: merge into target table
merge into edw_target.lh2_deltac_sum_c as tgt 
using 
( 
select  
shiftindex ,  
 shiftdate ,  
 site_code ,  
 cliid ,  
 ddbkey ,  
 year# ,  
 month# ,  
 months ,  
 shift ,  
 shift# ,  
 crew# ,  
 crew ,  
 harvhos ,  
 digtype ,  
 shvtype ,  
 dipper ,  
 trktype ,  
 payload ,  
 idletime ,  
 spottime ,  
 loadtime ,  
 dumpingtime ,  
 crusheridle ,  
 travelempty ,  
 travelloaded ,  
 totalcycle ,  
 calctravempty ,  
 calctravloaded ,  
 hos ,  
 timefull ,  
 timefull_ts ,
 timeloaded ,  
 excav ,  
 soperid ,  
 soper ,  
 truck ,  
 toperid ,  
 toper ,  
 load# ,  
 material ,  
 bench ,  
 unit# ,  
 unit ,  
 dumpname ,  
 idledelta ,  
 spotdelta ,  
 loaddelta ,  
 dumpdelta ,  
 et_delta ,  
 lt_delta ,  
 delta_c ,  
 toavgdeltac ,  
 tostdevdeltac ,  
 toavgidledelta ,  
 tostdevidledelta ,  
 toavgspotdelta ,  
 tostdevspotdelta ,  
 toavgloaddelta ,  
 tostdevloaddelta ,  
 toavgetdelta ,  
 tostdevetdelta ,  
 toavgltdelta ,  
 tostdevltdelta ,  
 toavgdumpdelta ,  
 tostdevdumpdelta ,  
 vtodeltac3 ,  
 load_seq_no ,  
 grade ,  
 extraload ,  
 loadtype ,  
 loadtons ,  
 measuretons_shovel ,  
 over_truck_flag ,  
 under_truck_flag ,  
 dump_flag ,  
 shovel_idle ,  
 crusher_dump_flag ,  
 dump_number ,  
 ldump_hos ,  
 dump_hos ,  
 dump_oper_id ,  
 shovel_eqmttype# ,  
 truck_eqmttype# ,  
 shvl_ops_prtl_equip_group_no#,
 trk_ops_prtl_equip_group_no#,
 shovel_idledelta ,  
 eliftup ,  
 eliftdown ,  
 distloaded ,  
 fliftup ,  
 fliftdown ,  
 distempty ,  
 emptyhaul ,  
 load_timearrive , 
 load_timearrive_ts ,
 timeload ,  
 timeload_ts ,
 beginspot ,  
 beginspot_ts ,
 hangtime ,  
 dump_timearrive ,  
 dump_timearrive_ts ,
 timedump , 
 timedump_ts ,
 timeempty ,  
 timeempty_ts ,
 material_name ,  
 start_date_ts ,
 end_date_ts,
 system_version ,  
 dw_logical_delete_flag ,  
 dw_modify_ts ,  
 dw_load_ts   
from  edw_stage.lh2_deltac_sum
qualify( row_number() over (partition by cliid,shiftindex,ddbkey order by toper desc,soper desc) =1)
) as stg 
ON ( stg.shiftindex = tgt.shiftindex and 
stg.cliid = tgt.cliid and 
stg.ddbkey = tgt.ddbkey 
 )

WHEN MATCHED THEN 
UPDATE SET 
 site_code =  stg.site_code , 
 shiftdate =  stg.shiftdate,
excav = stg.excav , 
truck = stg.truck,
 year# =  stg.year# , 
 month# =  stg.month# , 
 months =  stg.months , 
 shift =  stg.shift , 
 shift# =  stg.shift# , 
 crew# =  stg.crew# , 
 crew =  stg.crew , 
 harvhos =  stg.harvhos , 
 digtype =  stg.digtype , 
 shvtype =  stg.shvtype , 
 dipper =  stg.dipper , 
 trktype =  stg.trktype , 
 payload =  stg.payload , 
 idletime =  stg.idletime , 
 spottime =  stg.spottime , 
 loadtime =  stg.loadtime , 
 dumpingtime =  stg.dumpingtime , 
 crusheridle =  stg.crusheridle , 
 travelempty =  stg.travelempty , 
 travelloaded =  stg.travelloaded , 
 totalcycle =  stg.totalcycle , 
 calctravempty =  stg.calctravempty , 
 calctravloaded =  stg.calctravloaded , 
 hos =  stg.hos , 
 timefull =  stg.timefull , 
 timefull_ts =  stg.timefull_ts , 
 timeloaded =  stg.timeloaded , 
 soperid =  stg.soperid , 
 soper =  stg.soper , 
 toperid =  stg.toperid , 
 toper =  stg.toper , 
 load# =  stg.load# , 
 material =  stg.material , 
 bench =  stg.bench , 
 unit# =  stg.unit# , 
 unit =  stg.unit , 
 dumpname =  stg.dumpname , 
 idledelta =  stg.idledelta , 
 spotdelta =  stg.spotdelta , 
 loaddelta =  stg.loaddelta , 
 dumpdelta =  stg.dumpdelta , 
 et_delta =  stg.et_delta , 
 lt_delta =  stg.lt_delta , 
 delta_c =  stg.delta_c , 
 toavgdeltac =  stg.toavgdeltac , 
 tostdevdeltac =  stg.tostdevdeltac , 
 toavgidledelta =  stg.toavgidledelta , 
 tostdevidledelta =  stg.tostdevidledelta , 
 toavgspotdelta =  stg.toavgspotdelta , 
 tostdevspotdelta =  stg.tostdevspotdelta , 
 toavgloaddelta =  stg.toavgloaddelta , 
 tostdevloaddelta =  stg.tostdevloaddelta , 
 toavgetdelta =  stg.toavgetdelta , 
 tostdevetdelta =  stg.tostdevetdelta , 
 toavgltdelta =  stg.toavgltdelta , 
 tostdevltdelta =  stg.tostdevltdelta , 
 toavgdumpdelta =  stg.toavgdumpdelta , 
 tostdevdumpdelta =  stg.tostdevdumpdelta , 
 vtodeltac3 =  stg.vtodeltac3 , 
 load_seq_no =  stg.load_seq_no , 
 grade =  stg.grade , 
 extraload =  stg.extraload , 
 loadtype =  stg.loadtype , 
 loadtons =  stg.loadtons , 
 measuretons_shovel =  stg.measuretons_shovel , 
 over_truck_flag =  stg.over_truck_flag , 
 under_truck_flag =  stg.under_truck_flag , 
 dump_flag =  stg.dump_flag , 
 shovel_idle =  stg.shovel_idle , 
 crusher_dump_flag =  stg.crusher_dump_flag , 
 dump_number =  stg.dump_number , 
 ldump_hos =  stg.ldump_hos , 
 dump_hos =  stg.dump_hos , 
 dump_oper_id =  stg.dump_oper_id , 
 shovel_eqmttype# =  stg.shovel_eqmttype# , 
 truck_eqmttype# =  stg.truck_eqmttype# , 
 shvl_ops_prtl_equip_group_no# =  stg.shvl_ops_prtl_equip_group_no#,
 trk_ops_prtl_equip_group_no# =   stg.trk_ops_prtl_equip_group_no#,
 shovel_idledelta =  stg.shovel_idledelta , 
 eliftup =  stg.eliftup , 
 eliftdown =  stg.eliftdown , 
 distloaded =  stg.distloaded , 
 fliftup =  stg.fliftup , 
 fliftdown =  stg.fliftdown , 
 distempty =  stg.distempty , 
 emptyhaul =  stg.emptyhaul , 
 load_timearrive =  stg.load_timearrive , 
 load_timearrive_ts =  stg.load_timearrive_ts , 
 timeload =  stg.timeload , 
 timeload_ts =  stg.timeload_ts , 
 beginspot =  stg.beginspot , 
 beginspot_ts =  stg.beginspot_ts , 
 hangtime =  stg.hangtime , 
 dump_timearrive =  stg.dump_timearrive , 
 dump_timearrive_ts =  stg.dump_timearrive_ts , 
 timedump =  stg.timedump , 
 timedump_ts =  stg.timedump_ts , 
 timeempty =  stg.timeempty , 
 timeempty_ts =  stg.timeempty_ts , 
 material_name =  stg.material_name , 
 start_date_ts =  stg.start_date_ts ,
 end_date_ts =  stg.end_date_ts,
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
  stg.year# , 
  stg.month# , 
  stg.months , 
  stg.shift , 
  stg.shift# , 
  stg.crew# , 
  stg.crew , 
  stg.harvhos , 
  stg.digtype , 
  stg.shvtype , 
  stg.dipper , 
  stg.trktype , 
  stg.payload , 
  stg.idletime , 
  stg.spottime , 
  stg.loadtime , 
  stg.dumpingtime , 
  stg.crusheridle , 
  stg.travelempty , 
  stg.travelloaded , 
  stg.totalcycle , 
  stg.calctravempty , 
  stg.calctravloaded , 
  stg.hos , 
  stg.timefull , 
  stg.timefull_ts , 
  stg.timeloaded , 
  stg.excav , 
  stg.soperid , 
  stg.soper , 
  stg.truck , 
  stg.toperid , 
  stg.toper , 
  stg.load# , 
  stg.material , 
  stg.bench , 
  stg.unit# , 
  stg.unit , 
  stg.dumpname , 
  stg.idledelta , 
  stg.spotdelta , 
  stg.loaddelta , 
  stg.dumpdelta , 
  stg.et_delta , 
  stg.lt_delta , 
  stg.delta_c , 
  stg.toavgdeltac , 
  stg.tostdevdeltac , 
  stg.toavgidledelta , 
  stg.tostdevidledelta , 
  stg.toavgspotdelta , 
  stg.tostdevspotdelta , 
  stg.toavgloaddelta , 
  stg.tostdevloaddelta , 
  stg.toavgetdelta , 
  stg.tostdevetdelta , 
  stg.toavgltdelta , 
  stg.tostdevltdelta , 
  stg.toavgdumpdelta , 
  stg.tostdevdumpdelta , 
  stg.vtodeltac3 , 
  stg.load_seq_no , 
  stg.grade , 
  stg.extraload , 
  stg.loadtype , 
  stg.loadtons , 
  stg.measuretons_shovel , 
  stg.over_truck_flag , 
  stg.under_truck_flag , 
  stg.dump_flag , 
  stg.shovel_idle , 
  stg.crusher_dump_flag , 
  stg.dump_number , 
  stg.ldump_hos , 
  stg.dump_hos , 
  stg.dump_oper_id , 
  stg.shovel_eqmttype# , 
  stg.truck_eqmttype# , 
  stg.shvl_ops_prtl_equip_group_no#,
  stg.trk_ops_prtl_equip_group_no#,
  stg.shovel_idledelta , 
  stg.eliftup , 
  stg.eliftdown , 
  stg.distloaded , 
  stg.fliftup , 
  stg.fliftdown , 
  stg.distempty , 
  stg.emptyhaul , 
  stg.load_timearrive , 
  stg.load_timearrive_ts , 
  stg.timeload , 
  stg.timeload_ts , 
  stg.beginspot , 
  stg.beginspot_ts , 
  stg.hangtime , 
  stg.dump_timearrive , 
  stg.dump_timearrive_ts , 
  stg.timedump , 
  stg.timedump_ts , 
  stg.timeempty , 
  stg.timeempty_ts , 
  stg.material_name , 
  stg.start_date_ts ,
  stg.end_date_ts,
  stg.system_version , 
  stg.dw_logical_delete_flag , 
  stg.dw_modify_ts , 
  stg.dw_load_ts);

--Step 9: Capture the deleted rows
 
update edw_target.lh2_deltac_sum_c  as tgt 
set dw_logical_delete_flag = 'Y' ,
       dw_modify_ts = cast(proc_var_dw_modify_ts as timestamp(0))
where 
tgt.dw_logical_delete_flag <>'Y'
and not exists
      (select 1
      from edw_stage.lh2_deltac_sum as stg 
      where stg.shiftindex = tgt.shiftindex 
      and stg.cliid = tgt.cliid 
      and stg.ddbkey = tgt.ddbkey 
         )  
and  exists
     (
     select 'X'
     from edw_stage.lh2_deltac_sum  stg1 
     where  stg1.shiftindex = tgt.shiftindex
     and stg1.cliid = tgt.cliid
     )   ; 

begin request
--Step 10: Cleanup the target table 
delete   edw_target.lh2_deltac_sum_c  where dw_logical_delete_flag = 'Y' and dw_modify_ts <= ( cast(proc_var_dw_modify_ts as timestamp(0)) - interval '3' day );

--Step 11: update the di job contorl entry ts base table 
update edw_target.di_job_control_entry_ts_base 
set dw_load_ts =  cast(proc_var_dw_modify_ts as timestamp(0)),
extract_low_ts =  proc_var_di_last_run_ts_no_look_back  ,
extract_high_ts =  coalesce ( (select max(dw_load_ts) from edw_stage.lh2_deltac_sum), proc_var_di_last_run_ts_no_look_back )
where job_name ='job_lh2_deltac_sum_c_sp';
end request;

end;