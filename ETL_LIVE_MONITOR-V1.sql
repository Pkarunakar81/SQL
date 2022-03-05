


SELECT a.Package_name,Total_Count,Loaded_count,(Total_Count-Loaded_count)YET_TO_LOAD
,Table_Name_ETL--,[Net Address],[LOGIN_NAME],Command
,Session_ID,[Blocked By],LoadStartTime Today_start_time,LoadStartTime1 Yesterday_start_time--,procedurename
--,most_recent_sql_handle,sql_handle--,plan_handle
FROM (
			select Package_name,COUNT(DISTINCT TABLE_NAME)Total_Count,COUNT(DISTINCT B.taskname)Loaded_count,MIN(B.LoadStartTime)LoadStartTime,MIN(C.LoadStartTime)LoadStartTime1
			FROM INTDATA.ETL_LOADS_MONITOR A
			LEFT JOIN (SELECT distinct taskname,MIN(LoadStartTime)LoadStartTime FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate())
			GROUP BY taskname) B ON A.TABLE_NAME=B.TaskName
			LEFT JOIN (SELECT distinct taskname,MIN(LoadStartTime)LoadStartTime FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate()-1)
			GROUP BY taskname) C ON A.TABLE_NAME=C.TaskName
			WHERE FREQUENCY='DAILY'-- or package_name in ('PROCEDURES','PROCEDURES_1') 
			GROUP BY Package_name
)A
left join (

			select Package_name,b.*
			FROM INTDATA.ETL_LOADS_MONITOR A
			inner JOIN (


						SELECT 
						--case when procedurename is not null then 
						coalesce((REPLACE(REPLACE(RIGHT(left(most_recent_sql_handle,len(most_recent_sql_handle)-(len(most_recent_sql_handle)
						-CHARINDEX('](', most_recent_sql_handle,2))),(len(left(most_recent_sql_handle,len(most_recent_sql_handle)
						-(len(most_recent_sql_handle)-CHARINDEX('](', most_recent_sql_handle,2))))-CHARINDEX('].[', left(most_recent_sql_handle
						,len(most_recent_sql_handle)-(len(most_recent_sql_handle)-CHARINDEX('](', most_recent_sql_handle,2))))-1)),'[',''),']','')
						),(
						REPLACE(REPLACE(RIGHT(left(sql_handle,len(sql_handle)-(len(sql_handle)
						-CHARINDEX('](', sql_handle,2))),(len(left(sql_handle,len(sql_handle)
						-(len(sql_handle)-CHARINDEX('](', sql_handle,2))))-CHARINDEX('].[', left(sql_handle
						,len(sql_handle)-(len(sql_handle)-CHARINDEX('](', sql_handle,2))))-1)),'[',''),']','')
						),(
						REPLACE(REPLACE(RIGHT(left(plan_handle,len(plan_handle)-(len(plan_handle)
						-CHARINDEX('](', plan_handle,2))),(len(left(plan_handle,len(plan_handle)
						-(len(plan_handle)-CHARINDEX('](', plan_handle,2))))-CHARINDEX('].[', left(plan_handle
						,len(plan_handle)-(len(plan_handle)-CHARINDEX('](', plan_handle,2))))-1)),'[',''),']','')))
						--else procedurename end 
						Table_Name_ETL

						,isnull([Net Address],[Host Name])[Net Address],[Blocked By],LOGIN_NAME,Session_ID
						,Command,most_recent_sql_handle,sql_handle,plan_handle--,procedurename

						FROM (
						SELECT s.[Session_ID],s.LOGIN_NAME,        
						case when p.dbid=0 then N'' else ISNULL(db_name(p.dbid),N'') end [Database],
						ISNULL(r.command, N'') [Command],
						P.blocked [Blocked By],
						--object_name(sqltext.objectid,sqltext.dbid) as procedurename,
						ISNULL(s.host_name, N'')[Host Name]
						,ISNULL(c.client_net_address, N'')	[Net Address]
						,(SELECT text FROM sys.dm_exec_sql_text(r.sql_handle))sql_handle
						,(SELECT text FROM sys.dm_exec_sql_text(most_recent_sql_handle))most_recent_sql_handle
						,(SELECT text FROM sys.dm_exec_sql_text(r.plan_handle))plan_handle
						FROM sys.dm_exec_sessions s 
						LEFT OUTER JOIN sys.dm_exec_connections(nolock) c ON (s.session_id = c.session_id) 
						LEFT OUTER JOIN sys.dm_exec_requests(nolock) r ON (s.session_id = r.session_id)
						--cross apply sys.dm_exec_sql_text(sql_handle) as sqltext
						LEFT OUTER JOIN sys.sysprocesses(nolock) p ON (s.session_id = p.spid)
						where s.session_id>50 )a 
						WHERE [Host Name]='DC-ETL' AND
						(sql_handle LIKE 'INSERT%BULK%' OR most_recent_sql_handle LIKE 'INSERT%BULK%'  OR plan_handle LIKE 'INSERT%BULK%')
						AND (sql_handle LIKE 'INSERT%' OR most_recent_sql_handle LIKE 'INSERT%'  OR plan_handle LIKE '%NSERT%')
						--AND session_id <>@@SPID	

			) B ON A.TABLE_NAME=CASE WHEN B.Table_Name_ETL='CBS_CMG' THEN 'CBS_INT_CMG' 
								when B.Table_Name_ETL='LIABILITIES' then 'LIABLITIES' ELSE B.Table_Name_ETL END
			WHERE FREQUENCY='DAILY' --or package_name in ('PROCEDURES','PROCEDURES_1') --GROUP BY Package_name

) c ON A.Package_name=c.Package_name 
WHERE Total_Count<>Loaded_count
union all
SELECT a.Package_name,Total_Count,Loaded_count,(Total_Count-Loaded_count)YET_TO_LOAD
,Table_Name_ETL--,[Net Address],[LOGIN_NAME],Command
,Session_ID,[Blocked By],LoadStartTime Yesterday_start_time,LoadStartTime1 Today_start_time
FROM (
			select Package_name,COUNT(DISTINCT TABLE_NAME)Total_Count,COUNT(DISTINCT B.taskname)Loaded_count,MIN(B.LoadStartTime)LoadStartTime,MIN(C.LOADSTARTTIME)LOADSTARTTIME1
			FROM INTDATA.ETL_LOADS_MONITOR A
			LEFT JOIN (SELECT distinct taskname,MIN(LOADSTARTTIME)LOADSTARTTIME FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate()) GROUP BY taskname) B ON A.TABLE_NAME=B.TaskName
			LEFT JOIN (SELECT distinct taskname,MIN(LOADSTARTTIME)LOADSTARTTIME FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate()-1) GROUP BY taskname) C ON A.TABLE_NAME=C.TaskName
			WHERE package_name in ('PROCEDURES','PROCEDURES_1') 
			GROUP BY Package_name
)A
left join (

			select Package_name,b.*
			FROM INTDATA.ETL_LOADS_MONITOR A
			inner JOIN (


						SELECT 
						procedurename Table_Name_ETL

						,isnull([Net Address],[Host Name])[Net Address],[Blocked By],LOGIN_NAME,Session_ID
						,Command,most_recent_sql_handle,sql_handle,plan_handle--,procedurename

						FROM (
						SELECT distinct s.[Session_ID],s.LOGIN_NAME,        
						case when p.dbid=0 then N'' else ISNULL(db_name(p.dbid),N'') end [Database],
						ISNULL(r.command, N'') [Command],
						P.blocked [Blocked By],
						object_name(sqltext.objectid,sqltext.dbid) as procedurename,
						ISNULL(s.host_name, N'')[Host Name]
						,ISNULL(c.client_net_address, N'')	[Net Address]
						,(SELECT text FROM sys.dm_exec_sql_text(r.sql_handle))sql_handle
						,(SELECT text FROM sys.dm_exec_sql_text(most_recent_sql_handle))most_recent_sql_handle
						,(SELECT text FROM sys.dm_exec_sql_text(r.plan_handle))plan_handle
						FROM sys.dm_exec_sessions s 
						LEFT OUTER JOIN sys.dm_exec_connections(nolock) c ON (s.session_id = c.session_id) 
						LEFT OUTER JOIN sys.dm_exec_requests(nolock) r ON (s.session_id = r.session_id)
						cross apply sys.dm_exec_sql_text(sql_handle) as sqltext
						LEFT OUTER JOIN sys.sysprocesses(nolock) p ON (s.session_id = p.spid)
						where s.session_id>50 )a 
						WHERE [Host Name]='DC-ETL' --AND
						--(sql_handle LIKE 'INSERT%BULK%' OR most_recent_sql_handle LIKE 'INSERT%BULK%'  OR plan_handle LIKE 'INSERT%BULK%')
						--AND (sql_handle LIKE 'INSERT%' OR most_recent_sql_handle LIKE 'INSERT%'  OR plan_handle LIKE '%NSERT%')
						--AND session_id <>@@SPID	

			) B ON A.TABLE_NAME=CASE WHEN B.Table_Name_ETL='CBS_CMG' THEN 'CBS_INT_CMG' ELSE B.Table_Name_ETL END
			WHERE package_name in ('PROCEDURES','PROCEDURES_1') --GROUP BY Package_name

) c ON A.Package_name=c.Package_name 
WHERE Total_Count<>Loaded_count
union 

select 'Total_Pending' Package_name,sum(Total_Count)Total_Count,sum(Loaded_count)Loaded_count
,(sum(Total_Count)-sum(Loaded_count))YET_TO_LOAD
,null Table_Name_ETL,null Session_ID,null [Blocked By],null Today_start_time,null Yesterday_start_time 
from (
SELECT a.Package_name,Total_Count,Loaded_count,(Total_Count-Loaded_count)YET_TO_LOAD
,Table_Name_ETL--,[Net Address],[LOGIN_NAME],Command
,Session_ID,[Blocked By],LoadStartTime Today_start_time,LoadStartTime1 Yesterday_start_time--,procedurename
--,most_recent_sql_handle,sql_handle--,plan_handle
FROM (
			select Package_name,COUNT(DISTINCT TABLE_NAME)Total_Count,COUNT(DISTINCT B.taskname)Loaded_count,MIN(B.LoadStartTime)LoadStartTime,MIN(C.LoadStartTime)LoadStartTime1
			FROM INTDATA.ETL_LOADS_MONITOR A
			LEFT JOIN (SELECT distinct taskname,MIN(LoadStartTime)LoadStartTime FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate())
			GROUP BY taskname) B ON A.TABLE_NAME=B.TaskName
			LEFT JOIN (SELECT distinct taskname,MIN(LoadStartTime)LoadStartTime FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate()-1)
			GROUP BY taskname) C ON A.TABLE_NAME=C.TaskName
			WHERE FREQUENCY='DAILY'-- or package_name in ('PROCEDURES','PROCEDURES_1') 
			GROUP BY Package_name
)A
left join (

			select Package_name,b.*
			FROM INTDATA.ETL_LOADS_MONITOR A
			inner JOIN (


						SELECT 
						--case when procedurename is not null then 
						coalesce((REPLACE(REPLACE(RIGHT(left(most_recent_sql_handle,len(most_recent_sql_handle)-(len(most_recent_sql_handle)
						-CHARINDEX('](', most_recent_sql_handle,2))),(len(left(most_recent_sql_handle,len(most_recent_sql_handle)
						-(len(most_recent_sql_handle)-CHARINDEX('](', most_recent_sql_handle,2))))-CHARINDEX('].[', left(most_recent_sql_handle
						,len(most_recent_sql_handle)-(len(most_recent_sql_handle)-CHARINDEX('](', most_recent_sql_handle,2))))-1)),'[',''),']','')
						),(
						REPLACE(REPLACE(RIGHT(left(sql_handle,len(sql_handle)-(len(sql_handle)
						-CHARINDEX('](', sql_handle,2))),(len(left(sql_handle,len(sql_handle)
						-(len(sql_handle)-CHARINDEX('](', sql_handle,2))))-CHARINDEX('].[', left(sql_handle
						,len(sql_handle)-(len(sql_handle)-CHARINDEX('](', sql_handle,2))))-1)),'[',''),']','')
						),(
						REPLACE(REPLACE(RIGHT(left(plan_handle,len(plan_handle)-(len(plan_handle)
						-CHARINDEX('](', plan_handle,2))),(len(left(plan_handle,len(plan_handle)
						-(len(plan_handle)-CHARINDEX('](', plan_handle,2))))-CHARINDEX('].[', left(plan_handle
						,len(plan_handle)-(len(plan_handle)-CHARINDEX('](', plan_handle,2))))-1)),'[',''),']','')))
						--else procedurename end 
						Table_Name_ETL

						,isnull([Net Address],[Host Name])[Net Address],[Blocked By],LOGIN_NAME,Session_ID
						,Command,most_recent_sql_handle,sql_handle,plan_handle--,procedurename

						FROM (
						SELECT s.[Session_ID],s.LOGIN_NAME,        
						case when p.dbid=0 then N'' else ISNULL(db_name(p.dbid),N'') end [Database],
						ISNULL(r.command, N'') [Command],
						P.blocked [Blocked By],
						--object_name(sqltext.objectid,sqltext.dbid) as procedurename,
						ISNULL(s.host_name, N'')[Host Name]
						,ISNULL(c.client_net_address, N'')	[Net Address]
						,(SELECT text FROM sys.dm_exec_sql_text(r.sql_handle))sql_handle
						,(SELECT text FROM sys.dm_exec_sql_text(most_recent_sql_handle))most_recent_sql_handle
						,(SELECT text FROM sys.dm_exec_sql_text(r.plan_handle))plan_handle
						FROM sys.dm_exec_sessions s 
						LEFT OUTER JOIN sys.dm_exec_connections(nolock) c ON (s.session_id = c.session_id) 
						LEFT OUTER JOIN sys.dm_exec_requests(nolock) r ON (s.session_id = r.session_id)
						--cross apply sys.dm_exec_sql_text(sql_handle) as sqltext
						LEFT OUTER JOIN sys.sysprocesses(nolock) p ON (s.session_id = p.spid)
						where s.session_id>50 )a 
						WHERE [Host Name]='DC-ETL' AND
						(sql_handle LIKE 'INSERT%BULK%' OR most_recent_sql_handle LIKE 'INSERT%BULK%'  OR plan_handle LIKE 'INSERT%BULK%')
						AND (sql_handle LIKE 'INSERT%' OR most_recent_sql_handle LIKE 'INSERT%'  OR plan_handle LIKE '%NSERT%')
						--AND session_id <>@@SPID	

			) B ON A.TABLE_NAME=CASE WHEN B.Table_Name_ETL='CBS_CMG' THEN 'CBS_INT_CMG' 
								when B.Table_Name_ETL='LIABILITIES' then 'LIABLITIES' ELSE B.Table_Name_ETL END
			WHERE FREQUENCY='DAILY' --or package_name in ('PROCEDURES','PROCEDURES_1') --GROUP BY Package_name

) c ON A.Package_name=c.Package_name 
WHERE Total_Count<>Loaded_count
union all
SELECT a.Package_name,Total_Count,Loaded_count,(Total_Count-Loaded_count)YET_TO_LOAD
,Table_Name_ETL--,[Net Address],[LOGIN_NAME],Command
,Session_ID,[Blocked By],LoadStartTime Yesterday_start_time,LoadStartTime1 Today_start_time
FROM (
			select Package_name,COUNT(DISTINCT TABLE_NAME)Total_Count,COUNT(DISTINCT B.taskname)Loaded_count,MIN(B.LoadStartTime)LoadStartTime,MIN(C.LOADSTARTTIME)LOADSTARTTIME1
			FROM INTDATA.ETL_LOADS_MONITOR A
			LEFT JOIN (SELECT distinct taskname,MIN(LOADSTARTTIME)LOADSTARTTIME FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate()) GROUP BY taskname) B ON A.TABLE_NAME=B.TaskName
			LEFT JOIN (SELECT distinct taskname,MIN(LOADSTARTTIME)LOADSTARTTIME FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate()-1) GROUP BY taskname) C ON A.TABLE_NAME=C.TaskName
			WHERE package_name in ('PROCEDURES','PROCEDURES_1') 
			GROUP BY Package_name
)A
left join (

			select Package_name,b.*
			FROM INTDATA.ETL_LOADS_MONITOR A
			inner JOIN (


						SELECT 
						procedurename Table_Name_ETL

						,isnull([Net Address],[Host Name])[Net Address],[Blocked By],LOGIN_NAME,Session_ID
						,Command,most_recent_sql_handle,sql_handle,plan_handle--,procedurename

						FROM (
						SELECT distinct s.[Session_ID],s.LOGIN_NAME,        
						case when p.dbid=0 then N'' else ISNULL(db_name(p.dbid),N'') end [Database],
						ISNULL(r.command, N'') [Command],
						P.blocked [Blocked By],
						object_name(sqltext.objectid,sqltext.dbid) as procedurename,
						ISNULL(s.host_name, N'')[Host Name]
						,ISNULL(c.client_net_address, N'')	[Net Address]
						,(SELECT text FROM sys.dm_exec_sql_text(r.sql_handle))sql_handle
						,(SELECT text FROM sys.dm_exec_sql_text(most_recent_sql_handle))most_recent_sql_handle
						,(SELECT text FROM sys.dm_exec_sql_text(r.plan_handle))plan_handle
						FROM sys.dm_exec_sessions s 
						LEFT OUTER JOIN sys.dm_exec_connections(nolock) c ON (s.session_id = c.session_id) 
						LEFT OUTER JOIN sys.dm_exec_requests(nolock) r ON (s.session_id = r.session_id)
						cross apply sys.dm_exec_sql_text(sql_handle) as sqltext
						LEFT OUTER JOIN sys.sysprocesses(nolock) p ON (s.session_id = p.spid)
						where s.session_id>50 )a 
						WHERE [Host Name]='DC-ETL' --AND
						--(sql_handle LIKE 'INSERT%BULK%' OR most_recent_sql_handle LIKE 'INSERT%BULK%'  OR plan_handle LIKE 'INSERT%BULK%')
						--AND (sql_handle LIKE 'INSERT%' OR most_recent_sql_handle LIKE 'INSERT%'  OR plan_handle LIKE '%NSERT%')
						--AND session_id <>@@SPID	

			) B ON A.TABLE_NAME=CASE WHEN B.Table_Name_ETL='CBS_CMG' THEN 'CBS_INT_CMG' ELSE B.Table_Name_ETL END
			WHERE package_name in ('PROCEDURES','PROCEDURES_1') --GROUP BY Package_name

) c ON A.Package_name=c.Package_name 
WHERE Total_Count<>Loaded_count)a

ORDER BY 5 desc,1 ASC



/*
select Package_name,COUNT(DISTINCT TABLE_NAME)Total_Count,COUNT(DISTINCT B.taskname)Loaded_count
,MIN(B.LoadStartTime)LoadStartTime,MIN(C.LoadStartTime)LoadStartTime1

FROM INTDATA.ETL_LOADS_MONITOR A
LEFT JOIN (SELECT distinct taskname,MIN(LoadStartTime)LoadStartTime FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
where convert(date,LoadEndTime)=convert(date,getdate())
GROUP BY taskname) B ON A.TABLE_NAME=B.TaskName
LEFT JOIN (SELECT distinct taskname,MIN(LoadStartTime)LoadStartTime FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
where convert(date,LoadEndTime)=convert(date,getdate()-1)
GROUP BY taskname) C ON A.TABLE_NAME=C.TaskName
WHERE FREQUENCY='DAILY' GROUP BY Package_name

SELECT CAST(GETDATE() AS TIME)

CBS_HTH_HTD_ACC		04:15
CBS_HTH_HTD_ACC		05:05
CBS_HTH_HTD_ACC		06:00
CBS_STAG_TARGET		06:30
CRM_CLIENT			02:00
FNCLE_LOANS			06:00
IC4_CRM				01:00
IC4_CRM				03:00
IC4_CRM				04:00
IC4_FNCLE			05:00
IC4_FNCLE			07:00
IEXCEED				06:00
IEXCEED				14:00
INT_CBS_DAILY1		03:00
INT_CBS_DAILY1		05:15
INT_CBS_DAILY1		06:02
INT_CBS_DAILY10		06:30
INT_CBS_DAILY11		06:45
INT_CBS_DAILY12		07:00
INT_CBS_DAILY13		07:15
INT_CBS_DAILY2		03:20
INT_CBS_DAILY2		04:30
INT_CBS_DAILY2		06:30
INT_CBS_DAILY3		03:40
INT_CBS_DAILY3		05:29
INT_CBS_DAILY4		04:00
INT_CBS_DAILY5		04:20
INT_CBS_DAILY6		04:40
INT_CBS_DAILY7		05:00
INT_CBS_DAILY8		05:20
INT_CBS_DAILY9		05:40
INT_CBS_SIGN		01:10
INT_CRM				01:00
INT_CRM				02:00
INT_CRM1			01:30
INT_CRM1			02:30
INT_CRM2			02:01
INT_CRM3			02:31
IW_OW				07:00
LeadHistory			00:56
Liabilities_crm_daily			07:05
Liabilities_crm_daily			07:40
Liabilities_crm_daily			08:00
Liabilities_crm_daily			08:36
Liabilities_crm_daily			09:14
LP_INTEGRATION			06:15
POSIDEX			07:23
RTT_CXL			04:45
*/