


DECLARE @html nvarchar(MAX),@html1 nvarchar(MAX),@html2 nvarchar(MAX)
,@html3 nvarchar(MAX)
,@html4 nvarchar(MAX)
,@html5 nvarchar(MAX)
,@html6 nvarchar(MAX)
,@html7 nvarchar(MAX)
,@html8 nvarchar(MAX);

EXEC [spQueryToHtmlTable_kar] @html = @html OUTPUT,  @query = N'
SELECT a.Package_name,Total_Count Total,Loaded_count Loaded,(Total_Count-Loaded_count)Pending
--,Table_Name_ETL--,[Net Address],[LOGIN_NAME],Command
,Session_ID Session,[Blocked By] Blocked,left(cast(LoadStartTime as time),8) starttime,left(cast(LoadStartTime1 as time),8) T_1_starttime--,Next_Run_Time--,procedurename
,case 
when Table_Name_ETL is not null then concat(Table_Name_ETL,'' is Running'' )
when Table_Name_ETL is null and Next_Run_Time is not null  and LoadStartTime is null then concat(''Package will start at '',Next_Run_Time) 
when Table_Name_ETL is null and Next_Run_Time is null then ''Package has failed''
when Table_Name_ETL is null and LoadStartTime is not null then 
			case when Next_Run_Time is not null then concat(''Package has failed at'',left(cast(LoadStartTime as time),8),'' Trigger'','', Next trigger is at '',Next_Run_Time) end

end Status

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
			WHERE FREQUENCY=''DAILY''-- or package_name in (''PROCEDURES'',''PROCEDURES_1'') 
			GROUP BY Package_name
)A
left join (

			select a.Package_name,b.*
			FROM INTDATA.ETL_LOADS_MONITOR A
			inner JOIN (


						SELECT 
						case when (sql_handle LIKE ''INSERT%BULK%'' OR most_recent_sql_handle LIKE ''INSERT%BULK%''  OR plan_handle LIKE ''INSERT%BULK%'')
						or (sql_handle LIKE ''INSERT%'' OR most_recent_sql_handle LIKE ''INSERT%''  OR plan_handle LIKE ''%NSERT%'')
						then 
						coalesce((REPLACE(REPLACE(RIGHT(left(most_recent_sql_handle,len(most_recent_sql_handle)-(len(most_recent_sql_handle)
						-CHARINDEX('']('', most_recent_sql_handle,2))),(len(left(most_recent_sql_handle,len(most_recent_sql_handle)
						-(len(most_recent_sql_handle)-CHARINDEX('']('', most_recent_sql_handle,2))))-CHARINDEX(''].['', left(most_recent_sql_handle
						,len(most_recent_sql_handle)-(len(most_recent_sql_handle)-CHARINDEX('']('', most_recent_sql_handle,2))))-1)),''['',''''),'']'','''')
						),(
						REPLACE(REPLACE(RIGHT(left(sql_handle,len(sql_handle)-(len(sql_handle)
						-CHARINDEX('']('', sql_handle,2))),(len(left(sql_handle,len(sql_handle)
						-(len(sql_handle)-CHARINDEX('']('', sql_handle,2))))-CHARINDEX(''].['', left(sql_handle
						,len(sql_handle)-(len(sql_handle)-CHARINDEX('']('', sql_handle,2))))-1)),''['',''''),'']'','''')
						),(
						REPLACE(REPLACE(RIGHT(left(plan_handle,len(plan_handle)-(len(plan_handle)
						-CHARINDEX('']('', plan_handle,2))),(len(left(plan_handle,len(plan_handle)
						-(len(plan_handle)-CHARINDEX('']('', plan_handle,2))))-CHARINDEX(''].['', left(plan_handle
						,len(plan_handle)-(len(plan_handle)-CHARINDEX('']('', plan_handle,2))))-1)),''['',''''),'']'','''')))
						 
						when sql_handle LIKE ''%merge%'' or most_recent_sql_handle LIKE ''%merge%'' or plan_handle LIKE ''%merge%''
						then 
						substring(plan_handle,CHARINDEX(''merge'', plan_handle,2),(CHARINDEX(''using'', plan_handle,2))-(CHARINDEX(''merge'', plan_handle,2)))
						else null
						end 
						Table_Name_ETL

						,isnull([Net Address],[Host Name])[Net Address],[Blocked By],LOGIN_NAME,Session_ID
						,Command,most_recent_sql_handle,sql_handle,plan_handle--,procedurename

						FROM (
						SELECT s.[Session_ID],s.LOGIN_NAME,        
						case when p.dbid=0 then N'''' else ISNULL(db_name(p.dbid),N'''') end [Database],
						ISNULL(r.command, N'''') [Command],
						P.blocked [Blocked By],
						--object_name(sqltext.objectid,sqltext.dbid) as procedurename,
						ISNULL(s.host_name, N'''')[Host Name]
						,ISNULL(c.client_net_address, N'''')	[Net Address]
						,(SELECT text FROM sys.dm_exec_sql_text(r.sql_handle))sql_handle
						,(SELECT text FROM sys.dm_exec_sql_text(most_recent_sql_handle))most_recent_sql_handle
						,(SELECT text FROM sys.dm_exec_sql_text(r.plan_handle))plan_handle
						FROM sys.dm_exec_sessions s 
						LEFT OUTER JOIN sys.dm_exec_connections(nolock) c ON (s.session_id = c.session_id) 
						LEFT OUTER JOIN sys.dm_exec_requests(nolock) r ON (s.session_id = r.session_id)
						--cross apply sys.dm_exec_sql_text(sql_handle) as sqltext
						LEFT OUTER JOIN sys.sysprocesses(nolock) p ON (s.session_id = p.spid)
						where s.session_id>50 )a 
						WHERE [Host Name]=''DC-ETL'' --AND
						--(sql_handle LIKE ''INSERT%BULK%'' OR most_recent_sql_handle LIKE ''INSERT%BULK%''  OR plan_handle LIKE ''INSERT%BULK%'')
						--AND (sql_handle LIKE ''INSERT%'' OR most_recent_sql_handle LIKE ''INSERT%''  OR plan_handle LIKE ''%NSERT%'')
						--or sql_handle LIKE ''%merge%''
						--AND session_id <>@@SPID	

			) B ON A.TABLE_NAME=CASE WHEN B.Table_Name_ETL=''CBS_CMG'' THEN ''CBS_INT_CMG'' 
								when B.Table_Name_ETL=''LIABILITIES'' then ''LIABLITIES'' ELSE B.Table_Name_ETL END

	

			WHERE FREQUENCY=''DAILY'' --or package_name in (''PROCEDURES'',''PROCEDURES_1'') --GROUP BY Package_name

) c ON A.Package_name=c.Package_name 

							
left join (select Package_Name,Time Next_Run_Time
 
from (
select *,row_number() over(partition by Package_Name order by time)rn 
from (
select ''CBS_HTH_HTD_ACC'' Package_Name,''04:15:00'' Time UNION ALL select ''CBS_HTH_HTD_ACC'',''05:05:00'' UNION ALL
select ''CBS_HTH_HTD_ACC'',''06:00:00'' UNION ALL select ''CBS_STAG_TARGET'',''06:30:00'' UNION ALL
select ''CRM_CLIENT'',''02:00:00'' UNION ALL select ''FNCLE_LOANS'',''06:00:00'' UNION ALL
select ''IC4_CRM'',''01:00:00'' UNION ALL select ''IC4_CRM'',''03:00:00'' UNION ALL
select ''IC4_CRM'',''04:00:00'' UNION ALL select ''IC4_FNCLE'',''05:00:00'' UNION ALL
select ''IC4_FNCLE'',''07:00:00'' UNION ALL select ''IEXCEED'',''06:00:00'' UNION ALL
select ''IEXCEED'',''14:00:00'' UNION ALL select ''INT_CBS_DAILY1'',''03:00:00'' UNION ALL
select ''INT_CBS_DAILY1'',''05:15:00'' UNION ALL select ''INT_CBS_DAILY1'',''06:02:00'' UNION ALL
select ''INT_CBS_DAILY10'',''06:30:00'' UNION ALL select ''INT_CBS_DAILY11'',''06:45:00'' UNION ALL
select ''INT_CBS_DAILY12'',''07:00:00'' UNION ALL select ''INT_CBS_DAILY13'',''07:15:00'' UNION ALL
select ''INT_CBS_DAILY2'',''03:20:00'' UNION ALL select ''INT_CBS_DAILY2'',''04:30:00'' UNION ALL
select ''INT_CBS_DAILY2'',''06:30:00'' UNION ALL select ''INT_CBS_DAILY3'',''03:40:00'' UNION ALL
select ''INT_CBS_DAILY3'',''05:29:00'' UNION ALL select ''INT_CBS_DAILY4'',''04:00:00'' UNION ALL
select ''INT_CBS_DAILY5'',''04:20:00'' UNION ALL select ''INT_CBS_DAILY6'',''04:40:00'' UNION ALL
select ''INT_CBS_DAILY7'',''05:00:00'' UNION ALL select ''INT_CBS_DAILY8'',''05:20:00'' UNION ALL
select ''INT_CBS_DAILY9'',''05:40:00'' UNION ALL select ''INT_CBS_SIGN'',''01:10:00'' UNION ALL
select ''INT_CRM'',''01:00:00'' UNION ALL select ''INT_CRM'',''02:00:00'' UNION ALL
select ''INT_CRM1'',''01:30:00'' UNION ALL select ''INT_CRM1'',''02:30:00'' UNION ALL
select ''INT_CRM2'',''02:01:00'' UNION ALL select ''INT_CRM3'',''02:31:00'' UNION ALL
select ''IW_OW'',''07:00:00'' UNION ALL select ''LeadHistory'',''00:56:00'' UNION ALL
select ''Liabilities_crm_daily'',''07:05:00'' UNION ALL select ''Liabilities_crm_daily'',''07:40:00'' UNION ALL
select ''Liabilities_crm_daily'',''08:00:00'' UNION ALL select ''Liabilities_crm_daily'',''08:36:00'' UNION ALL
select ''Liabilities_crm_daily'',''09:14:00'' UNION ALL select ''LP_INTEGRATION'',''06:15:00'' UNION ALL
select ''POSIDEX'',''07:23:00'' UNION ALL select ''RTT_CXL'',''04:45:00''
)a
where Time>=left(cast(getdate() as time),8) 
--where Time>=''06:15:00''
)a
where rn=1)y on a.Package_name=y.Package_Name
WHERE Total_Count<>Loaded_count
union all
SELECT a.Package_name,Total_Count,Loaded_count,(Total_Count-Loaded_count)YET_TO_LOAD
--,Table_Name_ETL--,[Net Address],[LOGIN_NAME],Command
,Session_ID,[Blocked By]--,LoadStartTime Yesterday_start_time,LoadStartTime1 Today_start_time
,left(cast(LoadStartTime as time),8) Today_start_time,left(cast(LoadStartTime1 as time),8) Yesterday_start_time--,null Next_Run_Time
,case 
when Table_Name_ETL is not null then concat(Table_Name_ETL,'' is Running'' )
else ''Package is not Started''
end Status


FROM (
			select Package_name,COUNT(DISTINCT TABLE_NAME)Total_Count,COUNT(DISTINCT B.taskname)Loaded_count,MIN(B.LoadStartTime)LoadStartTime,MIN(C.LOADSTARTTIME)LOADSTARTTIME1
			FROM INTDATA.ETL_LOADS_MONITOR A
			LEFT JOIN (SELECT distinct taskname,MIN(LOADSTARTTIME)LOADSTARTTIME FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate()) GROUP BY taskname) B ON A.TABLE_NAME=B.TaskName
			LEFT JOIN (SELECT distinct taskname,MIN(LOADSTARTTIME)LOADSTARTTIME FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate()-1) GROUP BY taskname) C ON A.TABLE_NAME=C.TaskName
			WHERE package_name in (''PROCEDURES'',''PROCEDURES_1'') 
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
						case when p.dbid=0 then N'''' else ISNULL(db_name(p.dbid),N'''') end [Database],
						ISNULL(r.command, N'''') [Command],
						P.blocked [Blocked By],
						object_name(sqltext.objectid,sqltext.dbid) as procedurename,
						ISNULL(s.host_name, N'''')[Host Name]
						,ISNULL(c.client_net_address, N'''')	[Net Address]
						,(SELECT text FROM sys.dm_exec_sql_text(r.sql_handle))sql_handle
						,(SELECT text FROM sys.dm_exec_sql_text(most_recent_sql_handle))most_recent_sql_handle
						,(SELECT text FROM sys.dm_exec_sql_text(r.plan_handle))plan_handle
						FROM sys.dm_exec_sessions s 
						LEFT OUTER JOIN sys.dm_exec_connections(nolock) c ON (s.session_id = c.session_id) 
						LEFT OUTER JOIN sys.dm_exec_requests(nolock) r ON (s.session_id = r.session_id)
						cross apply sys.dm_exec_sql_text(sql_handle) as sqltext
						LEFT OUTER JOIN sys.sysprocesses(nolock) p ON (s.session_id = p.spid)
						where s.session_id>50 )a 
						WHERE [Host Name]=''DC-ETL'' --AND
						--(sql_handle LIKE ''INSERT%BULK%'' OR most_recent_sql_handle LIKE ''INSERT%BULK%''  OR plan_handle LIKE ''INSERT%BULK%'')
						--AND (sql_handle LIKE ''INSERT%'' OR most_recent_sql_handle LIKE ''INSERT%''  OR plan_handle LIKE ''%NSERT%'')
						--AND session_id <>@@SPID	

			) B ON A.TABLE_NAME=CASE WHEN B.Table_Name_ETL=''CBS_CMG'' THEN ''CBS_INT_CMG'' ELSE B.Table_Name_ETL END
			WHERE package_name in (''PROCEDURES'',''PROCEDURES_1'') --GROUP BY Package_name

) c ON A.Package_name=c.Package_name 
WHERE Total_Count<>Loaded_count
/* union all

SELECT * FROM (
select CASE WHEN (sum(Total_Count)-sum(Loaded_count))>0 THEN ''Total_Pending''ELSE NULL END Package_name
,ISNULL(sum(Total_Count),NULL)Total_Count,ISNULL(sum(Loaded_count),NULL)Loaded_count
,ISNULL((sum(Total_Count)-sum(Loaded_count)),NULL)YET_TO_LOAD
--,null Table_Name_ETL
,null Session_ID,null [Blocked By],null Today_start_time,null Yesterday_start_time 
--, null Next_Run_Time
,null Status
from (
SELECT a.Package_name,Total_Count,Loaded_count,(Total_Count-Loaded_count)YET_TO_LOAD
--,NULL Table_Name_ETL--,[Net Address],[LOGIN_NAME],Command
,NULL Session_ID,NULL [Blocked By],NULL Today_start_time,NULL Yesterday_start_time--,procedurename
--,most_recent_sql_handle,sql_handle--,plan_handle
FROM (
			select Package_name,COUNT(DISTINCT TABLE_NAME)Total_Count,COUNT(DISTINCT B.taskname)Loaded_count,NULL LoadStartTime,NULL LoadStartTime1
			FROM INTDATA.ETL_LOADS_MONITOR A
			LEFT JOIN (SELECT distinct taskname,MIN(LoadStartTime)LoadStartTime FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate())
			GROUP BY taskname) B ON A.TABLE_NAME=B.TaskName
			
			WHERE FREQUENCY=''DAILY'' --or package_name in (''PROCEDURES'',''PROCEDURES_1'') --GROUP BY Package_name
			GROUP BY Package_name
			)A WHERE Total_Count<>Loaded_count
union all
					SELECT a.Package_name,Total_Count,Loaded_count,(Total_Count-Loaded_count)YET_TO_LOAD
					--,NULL Table_Name_ETL--,[Net Address],[LOGIN_NAME],Command
					,NULL Session_ID,NULL [Blocked By],NULL Yesterday_start_time,NULL Today_start_time
					FROM (
								select Package_name,COUNT(DISTINCT TABLE_NAME)Total_Count,COUNT(DISTINCT B.taskname)Loaded_count,NULL LoadStartTime,NULL LOADSTARTTIME1
								FROM INTDATA.ETL_LOADS_MONITOR A
								LEFT JOIN (SELECT distinct taskname,MIN(LOADSTARTTIME)LOADSTARTTIME FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
								where convert(date,LoadEndTime)=convert(date,getdate()) GROUP BY taskname) B ON A.TABLE_NAME=B.TaskName
								WHERE package_name in (''PROCEDURES'',''PROCEDURES_1'') 
								GROUP BY Package_name
					)A
								WHERE package_name in (''PROCEDURES'',''PROCEDURES_1'') --GROUP BY Package_name
								AND Total_Count<>Loaded_count 
)A 
)Z 
WHERE Total_Count IS NOT NULL */
',@orderBy = N'ORDER BY T_1_starttime asc,Package_name ASC';
--order by package_name''--, @orderBy = N''ORDER BY customer_id';


EXEC [spQueryToHtmlTable_kar] @html = @html1 OUTPUT,  @query = N'
select distinct ROW_NUMBER() OVER(ORDER BY GETDATE()) SLNO,TaskName--,PackageName,LoadEndTime
--,STATUS,LoadEndTime,Duration,NumberOfRecords_Source,NumberOfRecords_Dest
from (SELECT distinct a.TaskName,b.PackageName, CASE WHEN b.TaskName IS NULL THEN ''NOT_COMPLETED'' ELSE ''COMPLETED'' END AS STATUS 
,LoadStartTime,LoadEndTime,STUFF(CONVERT(VARCHAR(20),LoadEndTime-LoadStartTime,114),1,2
,DATEDIFF(hh,0,LoadEndTime-LoadStartTime)) Duration,NumberOfRecords_Source,NumberOfRecords_Dest
FROM  (select TABLE_NAME TaskName FROM INTDATA.ETL_LOADS_MONITOR 
where isnull(FREQUENCY,'''')<>''DAILY'' and package_name not in (''PROCEDURES'',''PROCEDURES_1'')
and isnull(FREQUENCY,'''') not in (''MONTHLY'',''WEEKLY'',''THIRD OF EVERY MONTH'',''FOURTH OF EVERY MONTH'')
) a left join (select * from intdata.Audit_ETLPkg 
where convert(date,LoadEndTime)=convert(date,getdate()) ) b on a.TaskName=b.TaskName
where  a.TaskName NOT IN (SELECT TABLE_NAME FROM INTDATA.ETL_TABLE_DETAILS)
union all
select ''COL JOB STATUS'',''SQL SERVER AGENT'', CASE WHEN (
select TOP 1 CAST(CompleatedTime AS DATE) FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''OD Table Compleated'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)=CAST(GETDATE() AS DATE)THEN ''COMPLETED'' ELSE ''NOT_COMPLETED'' END AS STATUS ,
(select TOP 1 CompleatedTime FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''OD Table Strated'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC),--LoadStartTime
(select TOP 1 CompleatedTime FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''OD Table Compleated'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)--LoadEndTime
, case when (select TOP 1 CAST(CompleatedTime AS date)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''OD Table Compleated'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)!=cast(getdate() as date) then ''0:00:00:000'' else STUFF(CONVERT(VARCHAR(20),(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''OD Table Compleated'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)-(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES  where stepname=''OD Table Strated''
--and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC),114),1,2,DATEDIFF(hh,0,(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES where stepname=''OD Table Compleated''
--and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)-(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME) FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''OD Table Strated'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)))end Duration,0,0

union all
select ''DDD JOB STATUS'',''SQL SERVER AGENT'', CASE WHEN (
select TOP 1 CAST(CompleatedTime AS DATE) FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''DDD_STEP_2_UPDATE_COMPLETED'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)!=CAST(GETDATE() AS DATE) or (select TOP 1 CompleatedTime
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES  where stepname=''DDD_TRUNCATE_COMPLETED''
--and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)>(select TOP 1 CompleatedTime
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''DDD_STEP_2_UPDATE_COMPLETED'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC) THEN ''NOT_COMPLETED'' ELSE ''COMPLETED'' END AS STATUS ,
(select TOP 1 CompleatedTime FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''DDD_TRUNCATE_COMPLETED'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC),--LoadStartTime
(select TOP 1 CompleatedTime FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''DDD_STEP_2_UPDATE_COMPLETED'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)--LoadEndTime
, case when (select TOP 1 CAST(CompleatedTime AS date)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''DDD_STEP_2_UPDATE_COMPLETED'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)!=cast(getdate() as date) or (select TOP 1 CompleatedTime
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES  where stepname=''DDD_TRUNCATE_COMPLETED''
--and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)>(select TOP 1 CompleatedTime
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''DDD_STEP_2_UPDATE_COMPLETED'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC) then ''0:00:00:000'' else 
STUFF(CONVERT(VARCHAR(20),(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''DDD_STEP_2_UPDATE_COMPLETED'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)-(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES  where stepname=''DDD_TRUNCATE_COMPLETED''
--and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC),114),1,2,DATEDIFF(hh,0,(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES where stepname=''DDD_STEP_2_UPDATE_COMPLETED''
--and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)-(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME) FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''DDD_TRUNCATE_COMPLETED'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)))end Duration,0,0

union all
select ''DUES VS COLL JOB STATUS'',''SQL SERVER AGENT'', CASE WHEN (
select TOP 1 CAST(CompleatedTime AS DATE) FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''Credit_Repayment_Table_Updates_4_End'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)=CAST(GETDATE() AS DATE)THEN ''COMPLETED'' ELSE ''NOT_COMPLETED'' END AS STATUS ,
(select TOP 1 CompleatedTime FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''Daily_MB_Table_Created'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC),--LoadStartTime
(select TOP 1 CompleatedTime FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''Credit_Repayment_Table_Updates_4_End'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)--LoadEndTime
, case when (select TOP 1 CAST(CompleatedTime AS date)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''Credit_Repayment_Table_Updates_4_End'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)!=cast(getdate() as date) then ''0:00:00:000'' else 
STUFF(CONVERT(VARCHAR(20),(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''Credit_Repayment_Table_Updates_4_End'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)-(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES  where stepname=''Daily_MB_Table_Created''
--and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC),114),1,2,DATEDIFF(hh,0,(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES where stepname=''Credit_Repayment_Table_Updates_4_End''
--and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)-(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME) FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''Daily_MB_Table_Created'' --and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)))end Duration,0,0
union all
select ''CKYC_DASHBOARD_REPORT'',''SQL SERVER AGENT'', CASE WHEN (
select CAST(execution_time AS DATE) from [10.20.101.168,4554].[CKYC].[DBO].Report_data_movement)=CAST(GETDATE() AS DATE)THEN ''COMPLETED'' ELSE ''NOT_COMPLETED'' END AS STATUS ,
(select execution_starttime from [10.20.101.168,4554].[CKYC].[DBO].Report_data_movement),--LoadStartTime
(select execution_time from [10.20.101.168,4554].[CKYC].[DBO].Report_data_movement)--LoadEndTime
, case when (select CAST(execution_time AS DATE) from [10.20.101.168,4554].[CKYC].[DBO].Report_data_movement)!=cast(getdate() as date) then ''0:00:00:000'' else 
STUFF(CONVERT(VARCHAR(20),(select execution_time from [10.20.101.168,4554].[CKYC].[DBO].Report_data_movement)-
(select execution_starttime from [10.20.101.168,4554].[CKYC].[DBO].Report_data_movement),114),1,2,
DATEDIFF(hh,0,(select execution_time from [10.20.101.168,4554].[CKYC].[DBO].Report_data_movement)-(select execution_starttime from [10.20.101.168,4554].[CKYC].[DBO].Report_data_movement)))end Duration,0,0
--order by status desc,LoadEndTime DESC
union all
SELECT distinct a.TaskName,b.PackageName, CASE WHEN b.TaskName IS NULL THEN ''NOT_COMPLETED'' ELSE ''COMPLETED'' END AS STATUS 
,LoadStartTime,LoadEndTime
,STUFF(CONVERT(VARCHAR(20),LoadEndTime-LoadStartTime,114),1,2
,DATEDIFF(hh,0,LoadEndTime-LoadStartTime)) Duration   
,NumberOfRecords_Source,NumberOfRecords_Dest
FROM  (select  ''CKYC_CRM_LEGAL_ENTITY'' TaskName union all
select  ''CKYC'' union all select  ''CKYC_CRM_LEGAL_ENTITY_RELATED_PERSON_DETAILS'' union all
select  ''CKYC_CRM_RELATED_PERSON_DETAILS'') a
left join (select * from [10.20.101.168,4554].[CKYC].[DBO].AUDIT_ETLPKG
where convert(date,LoadEndTime)=convert(date,getdate()) ) b on a.TaskName=b.TaskName
where  a.TaskName NOT IN (SELECT TABLE_NAME FROM INTDATA.ETL_TABLE_DETAILS)
)a 
where status=''NOT_COMPLETED'''


EXEC [spQueryToHtmlTable_kar] @html = @html2 OUTPUT,  @query = N'
						SELECT Session_ID,LOGIN_NAME,Command,[Database]
,[Blocked By],[Host Name],[Net Address],procedurename
,case when isnull(procedurename,'''')='''' and most_recent_sql_handle like ''insert bulk%'' then 
ltrim(left(most_recent_sql_handle,len(most_recent_sql_handle)
						-(len(most_recent_sql_handle)-CHARINDEX('']('', most_recent_sql_handle,2))))
						when isnull(procedurename,'''')<>'''' then procedurename
else ltrim(most_recent_sql_handle) end most_recent_sql_handle
 FROM
 (
 SELECT distinct s.[Session_ID],s.LOGIN_NAME,        
						case when p.dbid=0 then N'''' else ISNULL(db_name(p.dbid),N'''') end [Database],
						ISNULL(r.command, N'''') [Command],
						P.blocked [Blocked By],
						object_name(sqltext.objectid,sqltext.dbid) as procedurename,
						ISNULL(s.host_name, N'''')[Host Name]
						,ISNULL(c.client_net_address, N'''')	[Net Address]
						,case when object_name(sqltext.objectid,sqltext.dbid) is null then (SELECT text FROM sys.dm_exec_sql_text(r.sql_handle)) end sql_handle
						,case when object_name(sqltext.objectid,sqltext.dbid) is null then (SELECT text FROM sys.dm_exec_sql_text(most_recent_sql_handle)) end most_recent_sql_handle
						,case when object_name(sqltext.objectid,sqltext.dbid) is null then (SELECT text FROM sys.dm_exec_sql_text(r.plan_handle))end plan_handle
						FROM sys.dm_exec_sessions s 
						LEFT OUTER JOIN sys.dm_exec_connections(nolock) c ON (s.session_id = c.session_id) 
						LEFT OUTER JOIN sys.dm_exec_requests(nolock) r ON (s.session_id = r.session_id)
						cross apply sys.dm_exec_sql_text(sql_handle) as sqltext
						LEFT OUTER JOIN sys.sysprocesses(nolock) p ON (s.session_id = p.spid)
						where s.session_id>50 and s.session_id<>@@spid )a'

EXEC [spQueryToHtmlTable_kar] @html = @html3 OUTPUT,  @query = N'SELECT Session_ID,LOGIN_NAME,Command,[Database]
,[Blocked By],[Host Name],[Net Address],procedurename
,case when isnull(procedurename,'''')='''' and most_recent_sql_handle like ''insert bulk%'' then 
ltrim(left(most_recent_sql_handle,len(most_recent_sql_handle)
						-(len(most_recent_sql_handle)-CHARINDEX('']('', most_recent_sql_handle,2))))
						when isnull(procedurename,'''')<>'''' then procedurename
else ltrim(most_recent_sql_handle) end most_recent_sql_handle
 FROM TMP_KAR_ETL_MONITOR_38'

EXEC [spQueryToHtmlTable_kar] @html = @html4 OUTPUT,  @query = N'SELECT Session_ID,LOGIN_NAME,Command,[Database]
,[Blocked By],[Host Name],[Net Address],procedurename
,case when isnull(procedurename,'''')='''' and most_recent_sql_handle like ''insert bulk%'' then 
ltrim(left(most_recent_sql_handle,len(most_recent_sql_handle)
						-(len(most_recent_sql_handle)-CHARINDEX('']('', most_recent_sql_handle,2))))
						when isnull(procedurename,'''')<>'''' then procedurename
else ltrim(most_recent_sql_handle) end most_recent_sql_handle
 FROM TMP_KAR_ETL_MONITOR_150 where LOGIN_NAME=''stguser'''

 EXEC [spQueryToHtmlTable_kar] @html = @html5 OUTPUT,  @query = N'select SL_NO,a.StepName
FROM INTDATA.ETL_150_STEPS  a
left join (
SELECT StepName,CompletedTime FROM [10.20.100.150].[MISREPORT_DB].[dbo].[Series]
where convert(date,CompletedTime) = convert(date,getdate()) 
) b on a.StepName=b.StepName where b.StepName is null' 

 EXEC [spQueryToHtmlTable_kar] @html = @html6 OUTPUT,  @query = N'select * FROM TMP_KAR_ETL_MONITOR_67 where Total_Tables<>Loaded'

 
  EXEC [spQueryToHtmlTable_kar] @html = @html7 OUTPUT,  @query = N'	
	select * from (
	select CASE WHEN package_name in (''PROCEDURES'',''PROCEDURES_1'') THEN package_name ELSE SOURCE_SYSTEM END SOURCE
			,COUNT(DISTINCT TABLE_NAME)Total,COUNT(DISTINCT B.taskname)Loaded
			,(COUNT(DISTINCT TABLE_NAME)-COUNT(DISTINCT B.taskname))Pending
			FROM INTDATA.ETL_LOADS_MONITOR A
			LEFT JOIN (SELECT distinct taskname,MIN(LoadStartTime)LoadStartTime FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate())
			GROUP BY taskname) B ON A.TABLE_NAME=B.TaskName
			WHERE FREQUENCY=''DAILY'' or package_name in (''PROCEDURES'',''PROCEDURES_1'')
			GROUP BY CASE WHEN package_name in (''PROCEDURES'',''PROCEDURES_1'') THEN package_name ELSE SOURCE_SYSTEM END
			)a  where Total<>Loaded
UNION ALL
			select ''Total_Pending'' SOURCE_SYSTEM,SUM(Total_Count)Total_Count
			,SUM(Loaded_count)Loaded_count
			,(SUM(Total_Count)-SUM(Loaded_count))YET_TO_LOAD 
			from (select CASE WHEN package_name in (''PROCEDURES'',''PROCEDURES_1'') THEN package_name ELSE SOURCE_SYSTEM END SOURCE_SYSTEM
			,COUNT(DISTINCT TABLE_NAME)Total_Count,COUNT(DISTINCT B.taskname)Loaded_count
			,(COUNT(DISTINCT TABLE_NAME)-COUNT(DISTINCT B.taskname))YET_TO_LOAD
			FROM INTDATA.ETL_LOADS_MONITOR A
			LEFT JOIN (SELECT distinct taskname,MIN(LoadStartTime)LoadStartTime FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate())
			GROUP BY taskname) B ON A.TABLE_NAME=B.TaskName
			WHERE FREQUENCY=''DAILY'' or package_name in (''PROCEDURES'',''PROCEDURES_1'')
			GROUP BY CASE WHEN package_name in (''PROCEDURES'',''PROCEDURES_1'') THEN package_name ELSE SOURCE_SYSTEM END
			)a  where Total_Count<>Loaded_count

union all
			select * from (
			select ''Total_Count'' SOURCE_SYSTEM
			,COUNT(DISTINCT TABLE_NAME)Total_Count,COUNT(DISTINCT B.taskname)Loaded_count
			,(COUNT(DISTINCT TABLE_NAME)-COUNT(DISTINCT B.taskname))YET_TO_LOAD
			FROM INTDATA.ETL_LOADS_MONITOR A
			LEFT JOIN (SELECT distinct taskname,MIN(LoadStartTime)LoadStartTime FROM MISREPORT_DB.intdata.Audit_ETLPkg(NOLOCK) B 
			where convert(date,LoadEndTime)=convert(date,getdate())
			GROUP BY taskname) B ON A.TABLE_NAME=B.TaskName
			WHERE FREQUENCY=''DAILY'' or package_name in (''PROCEDURES'',''PROCEDURES_1'')
		)a  --where Total_Count<>Loaded_count
		'


  EXEC [spQueryToHtmlTable_kar] @html = @html8 OUTPUT,  @query = N'
select * from (
select ''DDD JOB STATUS'' Job_Name, CASE WHEN (
select TOP 1 CAST(CompleatedTime AS DATE) FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''DDD_STEP_2_UPDATE_COMPLETED'' ORDER BY 1 DESC)!=CAST(GETDATE() AS DATE) or (select TOP 1 CompleatedTime
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES  where stepname=''DDD_TRUNCATE_COMPLETED''
--and cast(CompleatedTime as date)=cast(getdate() as date)
ORDER BY 1 DESC)>(select TOP 1 CompleatedTime FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''DDD_STEP_2_UPDATE_COMPLETED''ORDER BY 1 DESC) THEN ''NOT_COMPLETED'' ELSE ''COMPLETED'' END AS STATUS ,
(select TOP 1 CompleatedTime FROM [10.10.101.38].[MIS_DB].[DBO].SERIES where stepname=''DDD_TRUNCATE_COMPLETED'' ORDER BY 1 DESC) LoadStartTime
,(select TOP 1 CompleatedTime FROM [10.10.101.38].[MIS_DB].[DBO].SERIES where stepname=''DDD_STEP_2_UPDATE_COMPLETED''
ORDER BY 1 DESC)LoadEndTime, case when (select TOP 1 CAST(CompleatedTime AS date)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES where stepname=''DDD_STEP_2_UPDATE_COMPLETED''ORDER BY 1 DESC)!=cast(getdate() as date) 
or (select TOP 1 CompleatedTime FROM [10.10.101.38].[MIS_DB].[DBO].SERIES  where stepname=''DDD_TRUNCATE_COMPLETED''
ORDER BY 1 DESC)>(select TOP 1 CompleatedTime FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''DDD_STEP_2_UPDATE_COMPLETED'' ORDER BY 1 DESC) then ''0:00:00:000'' else STUFF(CONVERT(VARCHAR(20),(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES  where stepname=''DDD_STEP_2_UPDATE_COMPLETED'' ORDER BY 1 DESC)-(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES  where stepname=''DDD_TRUNCATE_COMPLETED''
ORDER BY 1 DESC),114),1,2,DATEDIFF(hh,0,(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME)
FROM [10.10.101.38].[MIS_DB].[DBO].SERIES where stepname=''DDD_STEP_2_UPDATE_COMPLETED''
ORDER BY 1 DESC)-(select TOP 1 CAST(CompleatedTime AS SMALLDATETIME) FROM [10.10.101.38].[MIS_DB].[DBO].SERIES 
where stepname=''DDD_TRUNCATE_COMPLETED'' ORDER BY 1 DESC)))end Duration
)a where STATUS=''NOT_COMPLETED'''


select concat(
'<p>DDD Status</p>',@html8
,'<p>77 Server Tables Load(Source-WISE)</p>',@html7
,'<p>77 Server Tables Load</p>',@html
,'<p>Datamart Status</p>',@html5
,'<p>150  Server Activities</p>',@html4
,'<p>Others Status</p>',@html1
,'<p>67 Server Tables Load</p>',@html6
,'<p>77 Server Activities</p>',@html2
,'<p>38  Server Activities</p>',@html3
)