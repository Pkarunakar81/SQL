
-----------------query and cpu stats---------------------------------------
SELECT a.session_id,client_net_address,status,connect_time,last_read,last_write,EXECUTION_COUNT,
DB_NAME((SELECT dbid FROM sys.dm_exec_sql_text(b.sql_handle))) AS [Database],
TimeElapsed = CAST(GETDATE() - start_time AS TIME)
,b.total_elapsed_time / 1000000.0 AS total_seconds,LAST_EXECUTION_TIME
--percent_complete,(b.total_elapsed_time / execution_count) / 1000000.0 AS average_seconds,
--(total_logical_reads + total_logical_writes) / execution_count AS average_IO,
--(total_logical_reads + total_logical_writes) AS total_IO,
--execution_count AS execution_count,
,SUBSTRING ((SELECT text FROM sys.dm_exec_sql_text(b.sql_handle)),b.statement_start_offset/2,
(CASE WHEN b.statement_end_offset = -1
THEN LEN(CONVERT(NVARCHAR(MAX), (SELECT text FROM sys.dm_exec_sql_text(b.sql_handle)))) * 2
ELSE b.statement_end_offset END - b.statement_start_offset)/2) AS indivudual_query
,(SELECT text FROM sys.dm_exec_sql_text(b.sql_handle))sql_handle
,(SELECT text FROM sys.dm_exec_sql_text(most_recent_sql_handle))most_recent_sql_handle
,(SELECT text FROM sys.dm_exec_sql_text(b.plan_handle))plan_handle,*
--,Avg_CPU_Time
--,(select query_plan from sys.dm_exec_query_plan (0x02000000D80F61221C85FADD4F98252D10475C1BFE0081EE0000000000000000000000000000000000000000)) query_plan
FROM sys.dm_exec_connections a 
left join sys.dm_exec_requests  b 
on a.session_id=b.session_id
left join sys.dm_exec_query_stats c on b.sql_handle=c.sql_handle
--left join (
--select sql_handle,query_hash AS "Query Hash",
--			SUM(total_worker_time) / SUM(execution_count) AS "Avg_CPU_Time"
--			from sys.dm_exec_query_stats
--			group by sql_handle,query_hash
			
--			) d on b.sql_handle=d.sql_handle
where A.SESSION_ID='119'
--DBCC INPUTBUFFER (116) 
--SELECT text FROM sys.dm_exec_sql_text(b.sql_handle)
--select query_plan from sys.dm_exec_query_plan (0x030009009A41FF443DEAD30097A7000000000000000000000000000000000000000000000000000000000000)
-- sys.dm_exec_query_stats 

