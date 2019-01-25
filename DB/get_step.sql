delimiter $$

drop procedure if exists get_step;$$


CREATE PROCEDURE get_step()
BEGIN

declare MaxRetries int;
declare StackMode int;
declare processid int;
declare stepid int;
declare instanceid int;

set processid = null;
set stepid =null;

-- Get configurations

-- Max number of allowed retries for step
select cast(conf_val as UNSIGNED) into MaxRetries from t_confs where conf_id = 1;
-- New process priority or exsiting processes priority
select cast(conf_val as UNSIGNED) into StackMode from t_confs where conf_id = 2;

-- If new process priority
if StackMode = 0
then

  -- Look for waiting process
  select process_id into processid from t_process_def where ready = 1 and status = 0 and enabled = 1 limit 1;
  
  -- Lock process
  update t_process_def set status = 1 where process_id = processid;
  
  -- If witingt process found
  if processid > 0 
  then     
  
    -- Create process instance
    insert into t_process_log (process_id, status, start_time) select processid,1,CURRENT_TIMESTAMP() ;
    
    -- Insert firt steps
    insert into t_step_log (process_id, step_id, instance_id, status, retries,  start_time, setting, setting_id)
    select processid, sd.step_id,instance_id, 0, 0,  CURRENT_TIMESTAMP(), setting, sd.setting_id
    from 
    t_process_log pl 
    join t_step_def sd
    on pl.process_id = sd.process_id
    join t_step_flow sfl
    on sfl.process_id = sd .process_id
    join t_setting_def std
    on std.setting_id = sd.setting_id
	where sfl.prev_step_id = -1
	and sd.enabled = 1
	and (sd.setting_id , sd.step_id) in (select min(setting_id),step_id from t_step_def where process_id = processid);
    
    -- Return some waiting step of the process 
    select process_id, step_id, instance_id, setting,setting_id from t_step_log where status = 0 and process_id = processid  limit 1;
    
  else
    -- Look for waiting step 
    select step_id ,instance_id into stepid,instanceid from t_step_log where status = 0 limit 1;  
  
    -- Lock step
    update t_step_log set status = 1 where step_id = stepid and instance_id=instanceid;
    
	-- Return step
    select process_id, step_id, instance_id, setting,setting_id from t_step_log where status = 0 and step_id = stepid and instance_id=instanceid  and retries <= MaxRetries limit 1;
    
  end if;
  
else

  -- Look for waiting step 
  select step_id ,instance_id into stepid,instanceid from t_step_log where status = 0 limit 1;  
  
  -- Lock step
  update t_step_log set status = 1 where step_id = stepid and instance_id=instanceid;  
  
  -- if step found
  if stepid > 0
  then
  
  -- Return step
  select process_id, step_id, instance_id, setting,setting_id from t_step_log where status = 0 and step_id = stepid and instance_id=instanceid and retries <= MaxRetries limit 1;
  
  -- If step not found look for new process 
  else 
  
    -- Look for waiting process
	select process_id into processid from t_process_def where ready = 1 and status = 0 and enabled = 1 limit 1;
  
    -- Lock process
    update t_process_def set status = 1 where process_id = processid; 
    
    if processid > 0 
    then     
  
      -- Create process instance
      insert into t_process_log (process_id, status, start_time) select processid,1,CURRENT_TIMESTAMP() ;
    
      -- Insert firt steps
      insert into t_step_log (process_id, step_id, instance_id, status, retries,  start_time, setting,setting_id)
      select processid, sd.step_id,instance_id, 0, 0,  CURRENT_TIMESTAMP(), setting,sd.setting_id
      from 
      t_process_log pl 
      join t_step_def sd
      on pl.process_id = sd.process_id
      join t_step_flow sfl
      on sfl.process_id = sd.process_id
      join t_setting_def std
      on std.setting_id = sd.setting_id
      where sfl.prev_step_id = -1
      and sd.enabled = 1
      and (sd.setting_id , sd.step_id) in (select min(setting_id),step_id from t_step_def where process_id = processid);
    
      -- Return some waiting step of the process 
      select process_id, step_id, instance_id, setting ,setting_id from t_step_log where status = 0 and process_id = processid  limit 1;
      
      -- Nothing  to run
    else
  
	  select null as process_id, null as  step_id, null as  instance_id, null as  setting,null as setting_id;
      
	end if;
  
  end if;

end if;

END$$