
  SELECT      [Session ID]    = s.session_id
--   ,cast('<?query '+CHAR(13)+CHAR(10)+(SELECT text FROM sys.dm_exec_sql_text( r.sql_handle))+CHAR(13)+CHAR(10)+'--?>' as xml)sql_handle
--,cast('<?query '+CHAR(13)+CHAR(10)+(SELECT text FROM sys.dm_exec_sql_text(most_recent_sql_handle))+CHAR(13)+CHAR(10)+'--?>' as xml)most_recent_sql_handle
--,cast('<?query '+CHAR(13)+CHAR(10)+(SELECT text FROM sys.dm_exec_sql_text(r.plan_handle))+CHAR(13)+CHAR(10)+'--?>' as xml)plan_handle
			 
       , [User Process]  = CONVERT(CHAR(1), s.is_user_process),     
  [Login]         = s.login_name,        
  [Database]      = case when p.dbid=0 then N'' else ISNULL(db_name(p.dbid),N'') end,      [Task State]    = ISNULL(t.task_state, N''),     
   [Command]       = ISNULL(r.command, N''),     
   [Application]   = ISNULL(s.program_name, N''),     
   [Wait Time (ms)]     = ISNULL(w.wait_duration_ms, 0),     [Wait Type]     = ISNULL(w.wait_type, N''),     
   [Wait Resource] = ISNULL(w.resource_description, N''),     
    [Blocked By]    = ISNULL(CONVERT (varchar, w.blocking_session_id), ''),    
    [Head Blocker]  =           CASE               
  -- session has an active request, is blocked, but is blocking others or session is idle but has an open tran and is blocking others             
   WHEN r2.session_id IS NOT NULL AND (r.blocking_session_id = 0 OR r.session_id IS NULL) THEN '1'               
   -- session is either not blocking someone, or is blocking someone but is blocked by another party             
    ELSE ''          END,     
	 [Total CPU (ms)] = s.cpu_time,      [Total Physical I/O (MB)]   = (s.reads + s.writes) * 8 / 1024,
	      [Memory Use (KB)]  = s.memory_usage * (8192 / 1024),      [Open Transactions] = ISNULL(r.open_transaction_count,0),
		        [Login Time]    = s.login_time, 
       [Last Request Start Time] = s.last_request_start_time,     [Host Name]     = ISNULL(s.host_name, N''), 
	       [Net Address]   = ISNULL(c.client_net_address, N''),      [Execution Context ID] = ISNULL(t.exec_context_id, 0), 
		       [Request ID] = ISNULL(r.request_id, 0),     [Workload Group] = ISNULL(g.name, N''),     
			   [Profiled Session Id] = profiled_session_id  
			   
			   ,(SELECT text FROM sys.dm_exec_sql_text(r.sql_handle))sql_handle
			   ,(SELECT text FROM sys.dm_exec_sql_text(most_recent_sql_handle))most_recent_sql_handle
			   ,(SELECT text FROM sys.dm_exec_sql_text(r.plan_handle))plan_handle

--			   ,cast('<?query '+CHAR(13)+CHAR(10)+(SELECT text FROM sys.dm_exec_sql_text( r.sql_handle))+CHAR(13)+CHAR(10)+'--?>' as xml)sql_handle
--,cast('<?query '+CHAR(13)+CHAR(10)+(SELECT text FROM sys.dm_exec_sql_text(most_recent_sql_handle))+CHAR(13)+CHAR(10)+'--?>' as xml)most_recent_sql_handle
--,cast('<?query '+CHAR(13)+CHAR(10)+(SELECT text FROM sys.dm_exec_sql_text(r.plan_handle))+CHAR(13)+CHAR(10)+'--?>' as xml)plan_handle
--			   --most_recent_sql_handle,plan_handle
--,*
	   FROM sys.dm_exec_sessions s 
	   LEFT OUTER JOIN sys.dm_exec_connections(nolock) c ON (s.session_id = c.session_id) 
	    
	   LEFT OUTER JOIN sys.dm_exec_requests(nolock) r ON (s.session_id = r.session_id)  
	  -- cross apply sys.dm_exec_sql_text(sql_handle) as sqltext
	   LEFT OUTER JOIN sys.dm_os_tasks(nolock) t ON (r.session_id = t.session_id AND r.request_id = t.request_id)  
	   LEFT OUTER JOIN   (     
	    -- In some cases (e.g. parallel queries, also waiting for a worker), one thread can be flagged as       
		-- waiting for several different threads.  This will cause that thread to show up in multiple rows      
		 -- in our grid, which we don't want.  Use ROW_NUMBER to select the longest wait for each thread,       
		 -- and use it as representative of the other wait relationships this thread is involved in.       
		 SELECT *, ROW_NUMBER() OVER (PARTITION BY waiting_task_address ORDER BY wait_duration_ms DESC) AS row_num      
		 FROM sys.dm_os_waiting_tasks(nolock)   ) w ON (t.task_address = w.waiting_task_address) AND w.row_num = 1  
		 LEFT OUTER JOIN sys.dm_exec_requests(nolock) r2 ON (s.session_id = r2.blocking_session_id)  
		 LEFT OUTER JOIN sys.dm_resource_governor_workload_groups(nolock) g ON (g.group_id = s.group_id)  
		 LEFT OUTER JOIN sys.sysprocesses(nolock) p ON (s.session_id = p.spid)  
  LEFT OUTER JOIN (SELECT DISTINCT session_id profiled_session_id from sys.dm_exec_query_profiles(nolock) ) Q ON Q.profiled_session_id = s.session_id  
  where s.session_id>50
  ORDER BY s.session_id; 

