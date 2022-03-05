
select * from sys.sysprocesses where blocked>0
sp_who2
misreport_db..sp_WhoIsActive
--dbcc sqlperf(logspace)

select name,filename--,convert(decimal(12,2),round(a.size/128.000,2)) as FileSizeMB
,convert(decimal(12,2),round(a.size/128.000,2))/1024 as FileSizeGB
--,convert(decimal(12,2),round(fileproperty(a.name,'SpaceUsed')/128.000,2)) as SpaceUsedMB
,convert(decimal(12,2),round(fileproperty(a.name,'SpaceUsed')/128.000,2))/1024 as SpaceUsedGB
--,convert(decimal(12,2),round((a.size-fileproperty(a.name,'SpaceUsed'))/128.000,2)) as FreeSpaceMB 
,convert(decimal(12,2),round((a.size-fileproperty(a.name,'SpaceUsed'))/128.000,2))/1024 as FreeSpaceGB 
from dbo.sysfiles a


select ourbranchid,count(1)
from T_rpt_loans4_Summary group by ourbranchid order by ourbranchid desc

WITH profiled_sessions as (
	SELECT DISTINCT session_id profiled_session_id from sys.dm_exec_query_profiles
)
SELECT SUBSTRING(qt.TEXT, (er.statement_start_offset/2)+1,
((CASE er.statement_end_offset
WHEN -1 THEN DATALENGTH(qt.TEXT)
ELSE er.statement_end_offset
END - er.statement_start_offset)/2)+1) as [Query],
er.session_id as [Session Id],
er.cpu_time as [CPU (ms/sec)],
db.name as [Database Name],
er.total_elapsed_time as [Elapsed Time],
er.reads as [Reads],
er.writes as [Writes],
er.logical_reads as [Logical Reads],
er.row_count as [Row Count],
mg.granted_memory_kb as [Allocated Memory],
mg.used_memory_kb as [Used Memory],
mg.required_memory_kb as [Required Memory],
/* We must convert these to a hex string representation because they will be stored in a DataGridView, which can't handle binary cell values (assumes anything binary is an image) */
master.dbo.fn_varbintohexstr(er.plan_handle) AS [sample_plan_handle], 
er.statement_start_offset as [sample_statement_start_offset],
er.statement_end_offset as [sample_statement_end_offset],
profiled_session_id as [Profiled Session Id]
FROM 
sys.dm_exec_requests er
LEFT OUTER JOIN sys.dm_exec_query_memory_grants mg 
	ON er.session_id = mg.session_id
LEFT OUTER JOIN profiled_sessions
	ON profiled_session_id = er.session_id
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt,
sys.databases db
WHERE db.database_id = er.database_id
AND er.session_id  <> @@spid

use GOLIVE
select top 10 createdon, * from t_transaction(nolock)
order by 1 desc

/*

SELECT CIF_ID,FORACID,ACCT_STATUS,ACCT_NAME,ACCT_OPN_DATE,CLR_BAL_AMT 
FROM TBAADM.GAM A
INNER JOIN TBAADM.SMT B ON A.ACID=B.ACID
where ACCT_OPN_DATE>=TO_DATE('2021-12-28', 'YYYY-MM-DD')
;

create table ##temp_ka (VALUE VARCHAR(30))
="INSERT INTO ##temp_ka VALUES ('"&A2&"')"

create table ##temp_ka (VALUE VARCHAR(30),VALUE1 VARCHAR(30))
="INSERT INTO ##temp_ka VALUES ('"&A2&"','"&B2&"')"
select 'INSERT INTO ##TEMP_KA VALUES ('''+CUSTOMER_ID+''')'

------------tables---------------------
select OBJECT_NAME(OBJECT_ID) AS TableName,last_user_seek,last_user_scan,last_user_lookup,
 last_user_update,*
FROM sys.dm_db_index_usage_stats
where OBJECT_NAME(OBJECT_ID)='t_EligibilityBaseList_GL_Loyalties' 

select * from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE

SELECT Name
FROM sys.procedures
WHERE OBJECT_DEFINITION(OBJECT_ID) LIKE '%RPT_LIAB_AVG_DATA%'
----------------sps----------------------------------------------
SELECT d.object_id, d.database_id, OBJECT_NAME(object_id, database_id) 'proc name',   
    d.cached_time, d.last_execution_time, d.total_elapsed_time,  
    d.total_elapsed_time/d.execution_count AS [avg_elapsed_time],  
    d.last_elapsed_time, d.execution_count ,*
FROM sys.dm_exec_procedure_stats AS d 
where  OBJECT_NAME(object_id, database_id)='SP_COCO_BRANCH_BANKING_DAILY'
---------------------views--------------------------------------
select * from sys.objects 

SELECT
    definition--,
    --uses_ansi_nulls,
   -- uses_quoted_identifier,
   -- is_schema_bound,*
FROM
    sys.sql_modules
WHERE
    object_id
    = object_id(
            'USP_REQ_CKYC_KAR'
        );

		SP_HELPTEXT SP_HELPTEXT

CREATE TABLE ##CommentText
(LineId	int
 ,Text  nvarchar(255) collate catalog_default)

 SELECT * FROM ##CommentText

 INSERT ##CommentText VALUES
                (
                  isnull(@Line, N'') + isnull(SUBSTRING(@SyscomText, @BasePos, @AddOnLen), N''))
                select  @LineId = @LineId + 1,
                       @BasePos = @BasePos + @AddOnLen, @BlankSpaceAdded = 0

SELECT 
    OBJECT_DEFINITION(
        OBJECT_ID(
            'USP_REQ_CKYC_KAR'
        )
    ) view_info;
	*/
-----------------seqr query and cpu stats---------------------------------------
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
where A.SESSION_ID='111'
--DBCC INPUTBUFFER (116) 
--SELECT text FROM sys.dm_exec_sql_text(b.sql_handle)
--select query_plan from sys.dm_exec_query_plan (0x030009009A41FF443DEAD30097A7000000000000000000000000000000000000000000000000000000000000)
-- sys.dm_exec_query_stats 


-------------------------------QUERY FOR CHECKING BLOCKING QUERY-----------------
SELECT * FROM SYS.sysprocesses
WHERE blocked>0



--------------------trigger stats------------------------------

SELECT TOP 5 d.object_id, d.database_id, DB_NAME(database_id) AS 'database_name',   
    OBJECT_NAME(object_id, database_id) AS 'trigger_name', d.cached_time,  
    d.last_execution_time, d.total_elapsed_time,   
    d.total_elapsed_time/d.execution_count AS [avg_elapsed_time],   
    d.last_elapsed_time, d.execution_count  
FROM sys.dm_exec_trigger_stats AS d  
ORDER BY [total_worker_time] DESC;  
-----------------------------------session ids-------------------------
SELECT *
FROM sys.dm_exec_connections a 
left join sys.dm_exec_requests  b 
on a.session_id=b.session_id
left join sys.dm_exec_query_stats c on b.query_plan_hash=c.query_plan_hash
where client_net_address='10.10.228.129'

----------------------------------catched objects-------------------------------------
SELECT usecounts, cacheobjtype, objtype, text   
FROM sys.dm_exec_cached_plans   
CROSS APPLY sys.dm_exec_sql_text(plan_handle)   
WHERE usecounts > 1   
ORDER BY usecounts DESC;  
-----------------------------------sessions--------------------------------------
SELECT login_name ,COUNT(session_id) AS session_count   
FROM sys.dm_exec_sessions   
GROUP BY login_name;  
----------------------------------------------------------------MOST EXPENSIVE SESSSIONS-------------------------



WITH profiled_sessions as (
	SELECT DISTINCT session_id profiled_session_id from sys.dm_exec_query_profiles
)
SELECT SUBSTRING(qt.TEXT, (er.statement_start_offset/2)+1,
((CASE er.statement_end_offset
WHEN -1 THEN DATALENGTH(qt.TEXT)
ELSE er.statement_end_offset
END - er.statement_start_offset)/2)+1) as [Query],
er.session_id as [Session Id],
er.cpu_time as [CPU (ms/sec)],
db.name as [Database Name],
er.total_elapsed_time as [Elapsed Time],
er.reads as [Reads],
er.writes as [Writes],
er.logical_reads as [Logical Reads],
er.row_count as [Row Count],
mg.granted_memory_kb as [Allocated Memory],
mg.used_memory_kb as [Used Memory],
mg.required_memory_kb as [Required Memory],
/* We must convert these to a hex string representation because they will be stored in a DataGridView, which can't handle binary cell values (assumes anything binary is an image) */
master.dbo.fn_varbintohexstr(er.plan_handle) AS [sample_plan_handle], 
er.statement_start_offset as [sample_statement_start_offset],
er.statement_end_offset as [sample_statement_end_offset],
profiled_session_id as [Profiled Session Id]
FROM 
sys.dm_exec_requests er
LEFT OUTER JOIN sys.dm_exec_query_memory_grants mg 
	ON er.session_id = mg.session_id
LEFT OUTER JOIN profiled_sessions
	ON profiled_session_id = er.session_id
CROSS APPLY sys.dm_exec_sql_text(er.sql_handle) qt,
sys.databases db
WHERE db.database_id = er.database_id
AND er.session_id  <> @@spid

/*

   ----------------LOGINNAME---------------------------
   EXEC sp_who '148'; 
   --SHUTDOWN [ WITH NOWAIT ]   



   Sum(Switch(Fields!Religion.Value = "CHRISTIAN", Fields!DISB_QTR_NUM.Value),"SLBC_DATASET")

   =Sum(FIELDS!, "MAIN_DS")



=IIf(IsNothing()=True, 0,)
HASHBYTES('SHA2_512', ACCT_NAME)

--------------------------------------QUERY TO CHECK THE TABLE SIZE IN THE DATABASE------------------------------

SELECT  sc.name + '.' + t.NAME AS TableName,  
        p.[Rows],  
        ( SUM(a.total_pages) * 8 ) / 1024 AS TotalReservedSpaceMB, -- Number of total pages * 8KB size of each page in SQL Server  
        ( SUM(a.used_pages) * 8 ) / 1024 AS UsedDataSpaceMB,  
        ( SUM(a.data_pages) * 8 ) / 1024 AS FreeUnusedSpaceMB  
FROM    sys.tables t  
        INNER JOIN sys.schemas sc ON sc.schema_id = t.schema_id  
        INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id  
        INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID  
                                            AND i.index_id = p.index_id  
        INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id  
WHERE   t.type_desc = 'USER_TABLE'  
        AND i.index_id <= 1  --- Heap\ CLUSTERED
        --AND t.NAME='REQ_CKYC_KAR' -- Replace with valid table name
GROUP BY sc.name + '.' + t.NAME,  
        i.[object_id],i.index_id, i.name, p.[Rows]  
ORDER BY ( SUM(a.total_pages) * 8 ) / 1024 DESC  


exec sp_spaceused REQ_CKYC_KAR

SELECT
SCHEMA_NAME(tbl.schema_id) AS [Schema],
tbl.name AS [Name],
tbl.object_id AS [ID]
FROM
sys.tables AS tbl
--LEFT OUTER JOIN sys.periods as periods ON periods.object_id = tbl.object_id
LEFT OUTER JOIN sys.tables as historyTable ON historyTable.object_id = tbl.history_table_id
ORDER BY
[Schema] ASC,[Name] ASC

-----------------------------------tot check the space used-------------------------

SELECT
(SELECT SUM(CAST(df.size as float)) FROM sys.database_files AS df WHERE df.type in ( 0, 2, 4 ) ) AS [DbSize],
(SUM(a.total_pages) + (SELECT ISNULL(SUM(CAST(df.size as bigint)), 0) FROM sys.database_files AS df WHERE df.type = 2 )) AS [SpaceUsed]
,
((SELECT SUM(CAST(df.size as float)) FROM sys.database_files AS df WHERE df.type in ( 0, 2, 4 ) ) -(SUM(a.total_pages) + (SELECT ISNULL(SUM(CAST(df.size as bigint)), 0) FROM sys.database_files AS df WHERE df.type = 2 )))

FROM
sys.partitions p join sys.allocation_units a on p.partition_id = a.container_id left join sys.internal_tables it on p.object_id = it.object_id


sp_who2 active

DBCC SQLPERF(logspace) 

select
      name
    , filename
    , convert(decimal(12,2),round(a.size/128.000,2)) as FileSizeMB
    , convert(decimal(12,2),round(fileproperty(a.name,'SpaceUsed')/128.000,2)) as SpaceUsedMB
    , convert(decimal(12,2),round((a.size-fileproperty(a.name,'SpaceUsed'))/128.000,2)) as FreeSpaceMB
from dbo.sysfiles a

------------------------------query to check the blockages-----------------------

select * from sys.sysprocesses where blocked>0

------------------------------------------performance tuning things-------------------------


prefer between instead of in 
use count(1) instead of count(*)
never use sp_ in any procedure starting
set nocount on--- stop the rusult givenby insert update and delete--must use in a procedure
use FROM dbo.EMPLOYEE e WITH (INDEX (Clus_Index))  
Group_Id int Sparse // Sparse Column ---large amounts numbers of NULL and Zero then prefer Sparse Column--takes lesser space

use table variable/cte instead of temp table
3NF form normalization, then it can be called well-structured tables
prefer union all instead of union --union perform distinct operation
t
op will produce results faster than those listed at the bottom.
=
>, >=, <, <=
LIKE
<>
 function acts directly on a column, and the index cannot be used:
SELECT member_number, first_name, last_name  
FROM members  
WHERE DATEDIFF(yy,datofbirth,GETDATE()) > 21  
In the following example, a function has been separated from the column and an index can be used:
SELECT member_number, first_name, last_name  
FROM members  
WHERE dateofbirth < DATEADD(yy,-21,GETDATE())  
Each of the preceding queries produces the same results, but the second query will use an index because the function is not performed directly on the column

sET ROWCOUNT statement, if you need to return only the first n rows

Use schema name with object name

Schema name should be used with store procedure name because it will help to compile the plan. It will not search in another schema before deciding to use the cached plan. So always prefix your object with the schema name.

Inefficient

Select * from Customer where YEAR(AccountCreatedOn) == 2005 and  MONTH(AccountCreatedOn) = 6

Note that even though AccountCreatedOn has an index, the above query changes the WHERE clause in such a way that this index cannot be used anymore.

Efficient

Select * From Customer Where AccountCreatedOn between ‘6/1/2005’ and ‘6/30/2005’


What are the different query optimization techniques?
There are two most common query optimization techniques – cost-based optimization and rule (logic) based optimization. For large databases, a cost-based query optimization technique is useful as it table join methods to deliver the required output. Rule-based optimization combines two or more queries based on relational expressions.

there are analytics functions for this in SQL the names are Lag for previous row and lead for nest row
SELECT CIF_ID,NET_INTEREST_ACCRUED_TILL_DATE
,lead(NET_INTEREST_ACCRUED_TILL_DATE,1,0) over(order by GETDATE()) as diff
,LAG(NET_INTEREST_ACCRUED_TILL_DATE,1,0) over(order by GETDATE()) as diff1

 FROM ##LIAB(NOLOCK)

 --to find the birthday oday inthis week or other
 select DATEDIFF(day, '2021-09-09',DATEADD(year,DATEDIFF(year,'1996-05-19', CAST('2021-09-09' as date)) , '1996-05-19'))

need to learn cross apply

---to generate uniqueid
    select t.*,
      RelatedId = first_value(newid()) over (partition by cif_id order by cif_id rows unbounded preceding)
    from ##LIAB t



select a.*,(select min(myvalu) from  (values (val1),(val2),(val3)) as d(myvalu) )as  'max' from ##test a


select collationname(0x0904D00034)


SELECT Req.percent_complete AS PercentComplete
,CONVERT(NUMERIC(6,2),Req.estimated_completion_time/1000.0/60.0) AS MinutesUntilFinish
,DB_NAME(Req.database_id) AS DbName,
Req.session_id AS SPID, Txt.text AS Query,
Req.command AS SubQuery,
Req.start_time AS StartTime
,(CASE WHEN Req.estimated_completion_time < 1
THEN NULL
ELSE DATEADD(SECOND, Req.estimated_completion_time / 1000, GETDATE())
END) AS EstimatedFinishDate
,Req.[status] AS QueryState, Req.wait_type AS BlockingType,
Req.blocking_session_id AS BlockingSPID
FROM sys.dm_exec_requests AS Req
CROSS APPLY sys.dm_exec_sql_text(Req.[sql_handle]) AS Txt
WHERE Req.command IN ('BACKUP DATABASE','RESTORE DATABASE', 'BACKUP LOG', 'RESTORE LOG') OR Req.command LIKE 'DBCC%';




Normalize tables in a database
Use Try–Catch
BEGIN TRY  
--SQL Statement  
--DML Statement
--Select Statement
END TRY  
BEGIN CATCH  
--Error handling code 
--Your logic to handle error  
END CATCH 

Use schema name with object name for the objects

Use Sparse Column
 
Sparse columns provide better performance for NULL and Zero data. If you have any column that contains large amounts numbers of NULL and Zero then prefer Sparse Column instead of the default column of SQL Server. The sparse column takes lesser space than the regular column (without the SPARSE clause).
 
Example
Create Table Table_Name  
(  
Id int, //Default Column  
Group_Id int Sparse // Sparse Column  
)  


You can prefer the table variable instead of temp table


Use Full-text Index
 
If your query contains multiple wild card searches using LIKE(%%), then the use of Full-text Index can increase the performance. 
Full-text queries can include simple words and phrases or multiple forms of a word or phrase. A full-text query returns any document that 
contains at least one match (also known as a hit). A match occurs when a target document contains all the terms specified in the Full-text query 
and meets any other search conditions, such as the distance between the matching terms.




--------------------jobs running check
SELECT  job.name AS JOB_NAME,job.job_id AS ID,job.originating_server AS HOSTNAME,activity.run_requested_date AS RUN_DATE,
DATEDIFF(MINUTE, activity.run_requested_date,GETDATE()) AS TIME_TAKEN_MINUTES ,CASE WHEN activity.last_executed_step_id is null
THEN 'Step 1 executing' ELSE 'Step ' + convert(VARCHAR(20), last_executed_step_id + 1) + ' executing' END AS JOB_STEP
FROM    msdb.dbo.sysjobs_view job JOIN msdb.dbo.sysjobactivity activity ON job.job_id = activity.job_id
JOIN msdb.dbo.syssessions sess ON sess.session_id = activity.session_id
JOIN (SELECT MAX(agent_start_date) AS max_agent_start_date FROM msdb.dbo.syssessions) sess_max 
ON sess.agent_start_date = sess_max.max_agent_start_date
WHERE run_requested_date IS NOT NULL AND stop_execution_date IS NULL

---SSRS

SELECT sch.scheduleID,Reportname = c.Name ,ReportPath = c.Path,SubscriptionDesc=su.Description 
,Subscriptiontype=su.EventType ,su.LastStatus ,su.LastRunTime ,Schedulename=sch.Name 
,ScheduleType = sch.EventType ,ScheduleFrequency = CASE sch.RecurrenceType 
 WHEN 1 THEN 'Once'  WHEN 2 THEN 'Hourly'  WHEN 4 THEN 'Daily/Weekly' WHEN 5 THEN 'Monthly'  END ,su.Parameters 
FROM [DBPRDRPTCLUS\DCMUMDBPRDRPT].Reportserver.dbo.Subscriptions su 
JOIN [DBPRDRPTCLUS\DCMUMDBPRDRPT].Reportserver.dbo.Catalog c   ON su.Report_OID = c.ItemID 
JOIN [DBPRDRPTCLUS\DCMUMDBPRDRPT].Reportserver.dbo.ReportSchedule rsc   ON rsc.ReportID = c.ItemID  AND rsc.SubscriptionID = su.SubscriptionID 
JOIN [DBPRDRPTCLUS\DCMUMDBPRDRPT].Reportserver.dbo.Schedule Sch   ON rsc.ScheduleID = sch.ScheduleID 
where su.LastStatus<>'Disabled' and cast(su.LastRunTime as date) = cast(GETDATE() as date)--'22 Nov 2021'
--	and ItemID='1C0B3BC0-E64D-4B6A-B83C-A85FD6EA3CD5'
--cast(su.LastRunTime as datetime) > '19 sep 2021 16:00:00'
  --and su.SubscriptionID='cf525dfb-f60e-4185-be88-8decf6fe6dfe'
 --and c.Name like '%br_disb%' and su.subject 
and (LastStatus like '%failure%') or (LastStatus like 'error%' ) --or (LastStatus like 'pending%' )
 --and LastStatus like '%Abhay%Kataria%'
--WHERE  LastStatus like '%Email%'
ORDER BY LastRunTime DESC

---DYNAMIC SQL
---1
DECLARE @sqlCommand varchar(1000)
DECLARE @columnList varchar(75)
DECLARE @city varchar(75)

SET @columnList = 'AddressID, AddressLine1, City'
SET @city = '''London'''
SET @sqlCommand = 'SELECT ' + @columnList + ' FROM Person.Address WHERE City = ' + @city

EXEC (@sqlCommand)

---2
DECLARE @sqlCommand nvarchar(1000)
DECLARE @columnList varchar(75)
DECLARE @city varchar(75)

SET @columnList = 'AddressID, AddressLine1, City'
SET @city = 'London'
SET @sqlCommand = 'SELECT ' + @columnList + ' FROM Person.Address WHERE City = @city'

EXECUTE sp_executesql @sqlCommand, N'@city nvarchar(75)', @city = @city

Find SQL Server version with SERVERPROPERTY
One of our readers, Ben Pomicter, also suggested this method using the SERVERPROPERTY function.

SELECT
  CASE 
     WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion')) like '8%' THEN 'SQL2000'
     WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion')) like '9%' THEN 'SQL2005'
     WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion')) like '10.0%' THEN 'SQL2008'
     WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion')) like '10.5%' THEN 'SQL2008 R2'
     WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion')) like '11%' THEN 'SQL2012'
     WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion')) like '12%' THEN 'SQL2014'
     WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion')) like '13%' THEN 'SQL2016'     
     WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion')) like '14%' THEN 'SQL2017' 
     WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion')) like '15%' THEN 'SQL2019' 
     ELSE 'unknown'
  END AS MajorVersion,
  SERVERPROPERTY('ProductLevel') AS ProductLevel,
  SERVERPROPERTY('Edition') AS Edition,
  SERVERPROPERTY('ProductVersion') AS ProductVersion

 SELECT @@VERSION

 Row Count for all Tables in a Database
 ---1--faster way---

 SELECT
      QUOTENAME(SCHEMA_NAME(sOBJ.schema_id)) + '.' + QUOTENAME(sOBJ.name) AS [TableName]
      , SUM(sPTN.Rows) AS [RowCount]
FROM 
      sys.objects AS sOBJ
      INNER JOIN sys.partitions AS sPTN
            ON sOBJ.object_id = sPTN.object_id
WHERE
      sOBJ.type = 'U'
      AND sOBJ.is_ms_shipped = 0x0
      AND index_id < 2 -- 0:Heap, 1:Clustered
GROUP BY 
      sOBJ.schema_id
      , sOBJ.name
ORDER BY [TableName]
GO

--2--okay but slow
SELECT
      QUOTENAME(SCHEMA_NAME(sOBJ.schema_id)) + '.' + QUOTENAME(sOBJ.name) AS [TableName]
      , SUM(sdmvPTNS.row_count) AS [RowCount]
FROM
      sys.objects AS sOBJ
      INNER JOIN sys.dm_db_partition_stats AS sdmvPTNS
            ON sOBJ.object_id = sdmvPTNS.object_id
WHERE 
      sOBJ.type = 'U'
      AND sOBJ.is_ms_shipped = 0x0
      AND sdmvPTNS.index_id < 2
GROUP BY
      sOBJ.schema_id
      , sOBJ.name
ORDER BY [TableName]
GO

--3--okay but too slow
DECLARE @TableRowCounts TABLE ([TableName] VARCHAR(128), [RowCount] INT) ;
INSERT INTO @TableRowCounts ([TableName], [RowCount])
EXEC sp_MSforeachtable 'SELECT ''?'' [TableName], COUNT(*) [RowCount] FROM ?' ;
SELECT [TableName], [RowCount]
FROM @TableRowCounts
ORDER BY [TableName]
GO

--4---giving error
DECLARE @QueryString NVARCHAR(MAX) ;
SELECT @QueryString = COALESCE(@QueryString + ' UNION ALL ','')
                      + 'SELECT '
                      + '''' + QUOTENAME(SCHEMA_NAME(sOBJ.schema_id))
                      + '.' + QUOTENAME(sOBJ.name) + '''' + ' AS [TableName]
                      , COUNT(*) AS [RowCount] FROM '
                      + QUOTENAME(SCHEMA_NAME(sOBJ.schema_id))
                      + '.' + QUOTENAME(sOBJ.name) + ' WITH (NOLOCK) '
FROM sys.objects AS sOBJ
WHERE
      sOBJ.type = 'U'
      AND sOBJ.is_ms_shipped = 0x0
ORDER BY SCHEMA_NAME(sOBJ.schema_id), sOBJ.name ;
EXEC sp_executesql @QueryString
GO



--Script #2 - CROSS APPLY and INNER JOIN

SELECT * FROM Department D 
CROSS APPLY 
   ( 
   SELECT * FROM Employee E 
   WHERE E.DepartmentID = D.DepartmentID 
   ) A 
GO
 
SELECT * FROM Department D 
INNER JOIN Employee E ON D.DepartmentID = E.DepartmentID 
GO 

--Script #3 - OUTER APPLY and LEFT OUTER JOIN

SELECT * FROM Department D 
OUTER APPLY 
   ( 
   SELECT * FROM Employee E 
   WHERE E.DepartmentID = D.DepartmentID 
   ) A 
GO
 
SELECT * FROM Department D 
LEFT OUTER JOIN Employee E ON D.DepartmentID = E.DepartmentID 
GO 

the CONVERT function is not very flexible and we have limited date formats. In Microsoft SQL Server 2012 and later, 
the function FORMAT has been introduced which is much easier to use to format dates.

SELECT FORMAT (getdate(), 'dd-MM-yy') as date
GO

SELECT FORMAT (getdate(), 'dd/MM/yyyy ') as date	21/03/2021
SELECT FORMAT (getdate(), 'dd/MM/yyyy, hh:mm:ss ') as date	21/03/2021, 11:36:14
SELECT FORMAT (getdate(), 'dddd, MMMM, yyyy') as date	Wednesday, March, 2021
SELECT FORMAT (getdate(), 'MMM dd yyyy') as date	Mar 21 2021
SELECT FORMAT (getdate(), 'MM.dd.yy') as date	03.21.21
SELECT FORMAT (getdate(), 'MM-dd-yy') as date	03-21-21
SELECT FORMAT (getdate(), 'hh:mm:ss tt') as date	11:36:14 AM
SELECT FORMAT (getdate(), 'd','us') as date	03/21/2021
SELECT FORMAT (getdate(), 'yyyy-MM-dd hh:mm:ss tt') as date	2021-03-21 11:36:14 AM
SELECT FORMAT (getdate(), 'yyyy.MM.dd hh:mm:ss t') as date	2021.03.21 11:36:14 A
SELECT FORMAT (getdate(), 'dddd, MMMM, yyyy','es-es') as date --Spanish	domingo, marzo, 2021
SELECT FORMAT (getdate(), 'dddd dd, MMMM, yyyy','ja-jp') as date --Japanese

DECLARE @counter INT = 0
DECLARE @date DATETIME = '2006-12-30 00:38:54.840'

CREATE TABLE #dateFormats (dateFormatOption int, dateOutput nvarchar(40))

WHILE (@counter <= 150 )
BEGIN
   BEGIN TRY
      INSERT INTO #dateFormats
      SELECT CONVERT(nvarchar, @counter), CONVERT(nvarchar,getdate(), @counter) 
      SET @counter = @counter + 1
   END TRY
   BEGIN CATCH;
      SET @counter = @counter + 1
      IF @counter >= 150
      BEGIN
         BREAK
      END
   END CATCH
END

SELECT * FROM #dateFormats

---query to get the cpu

WITH DB_CPU AS
(SELECT	DatabaseID, 
		DB_Name(DatabaseID)AS [DatabaseName], 
		SUM(total_worker_time)AS [CPU_Time(Ms)] 
FROM	sys.dm_exec_query_stats AS qs 
CROSS APPLY(SELECT	CONVERT(int, value)AS [DatabaseID]  
			FROM	sys.dm_exec_plan_attributes(qs.plan_handle)  
			WHERE	attribute =N'dbid')AS epa GROUP BY DatabaseID) 
SELECT	ROW_NUMBER()OVER(ORDER BY [CPU_Time(Ms)] DESC)AS [SNO], 
	DatabaseName AS [DBName], [CPU_Time(Ms)], 
	CAST([CPU_Time(Ms)] * 1.0 /SUM([CPU_Time(Ms)]) OVER()* 100.0 AS DECIMAL(5, 2))AS [CPUPercent] 
FROM	DB_CPU 
WHERE	DatabaseID > 4 -- system databases 
	AND DatabaseID <> 32767 -- ResourceDB 
ORDER BY SNO OPTION(RECOMPILE); 


EXEC sp_testlinkedserver N'10.20.101.67,4554'
*/


---------------------query to check the backup----


SELECT r.session_id,r.command,CONVERT(NUMERIC(6,2),r.percent_complete)
AS [Percent Complete],CONVERT(VARCHAR(20),DATEADD(ms,r.estimated_completion_time,GetDate()),20) AS [ETA Completion Time],
CONVERT(NUMERIC(10,2),r.total_elapsed_time/1000.0/60.0) AS [Elapsed Min],
CONVERT(NUMERIC(10,2),r.estimated_completion_time/1000.0/60.0) AS [ETA Min],
CONVERT(NUMERIC(10,2),r.estimated_completion_time/1000.0/60.0/60.0) AS [ETA Hours],
CONVERT(VARCHAR(1000),(SELECT SUBSTRING(text,r.statement_start_offset/2,
CASE WHEN r.statement_end_offset = -1 THEN 1000 ELSE (r.statement_end_offset-r.statement_start_offset)/2 END)
FROM sys.dm_exec_sql_text(sql_handle))) AS [SQL]
FROM sys.dm_exec_requests r WHERE command IN ('RESTORE DATABASE','BACKUP DATABASE')

;WITH CTE_Backup AS
(
SELECT  database_name,backup_start_date,type,physical_device_name,backup_finish_date
       ,Row_Number() OVER(PARTITION BY database_name,BS.type
        ORDER BY backup_start_date DESC) AS RowNum
FROM    msdb..backupset BS
JOIN    msdb.dbo.backupmediafamily BMF
ON      BS.media_set_id=BMF.media_set_id
)
SELECT      D.name
           ,ISNULL(CONVERT(VARCHAR,backup_start_date),'No backups') AS last_backup_START_time
		   ,ISNULL(CONVERT(VARCHAR,backup_finish_date),'No backups')  AS Last_BackUp_END_Time
		   ,STUFF(CONVERT(VARCHAR(20),backup_finish_date-backup_start_date,114),1,2,DATEDIFF(hh,0,backup_finish_date-backup_start_date)) Backup_
           ,D.recovery_model_desc
           ,state_desc,
            CASE WHEN type ='D' THEN 'Full database'
            WHEN type ='I' THEN 'Differential database'
            WHEN type ='L' THEN 'Log'
            WHEN type ='F' THEN 'File or filegroup'
            WHEN type ='G' THEN 'Differential file'
            WHEN type ='P' THEN 'Partial'
            WHEN type ='Q' THEN 'Differential partial'
            ELSE 'Unknown' END AS backup_type
           ,physical_device_name
FROM        sys.databases D
LEFT JOIN   CTE_Backup CTE
ON          D.name = CTE.database_name
AND         RowNum = 1
ORDER BY    D.name,type


-------------------------Identifying Databases Which Haven’t Been Backed Up Recently

;WITH CTE_Backup AS
(
SELECT   database_name,backup_start_date,type,is_readonly,physical_device_name
        ,Row_Number() OVER(PARTITION BY database_name
         ORDER BY backup_start_date DESC) AS RowNum
FROM     msdb..backupset BS
JOIN     msdb.dbo.backupmediafamily BMF
ON       BS.media_set_id=BMF.media_set_id
)
SELECT      D.name
           ,ISNULL(CONVERT(VARCHAR,backup_start_date),'No backups') AS last_backup_time
           ,D.recovery_model_desc
           ,state_desc
           ,physical_device_name
FROM        sys.databases D
LEFT JOIN   CTE_Backup CTE
ON          D.name = CTE.database_name
AND         RowNum = 1
WHERE       ( backup_start_date IS NULL OR backup_start_date < DATEADD(dd,-7,GetDate()) )
ORDER BY    D.name,type

--------------Missing Transaction Log Backups

;WITH CTE_Backup AS
(
SELECT   database_name,backup_start_date,type,is_readonly,physical_device_name
        ,Row_Number() OVER(PARTITION BY database_name,BS.type
         ORDER BY backup_start_date DESC) AS RowNum
FROM     msdb..backupset BS
JOIN     msdb.dbo.backupmediafamily BMF
ON       BS.media_set_id=BMF.media_set_id
WHERE    type='L'
)
SELECT      D.name
           ,ISNULL(CONVERT(VARCHAR,backup_start_date),'No log backups') AS last_backup_time
           ,D.recovery_model_desc
           ,state_desc
           ,physical_device_name
FROM        sys.databases D
LEFT JOIN   CTE_Backup CTE
ON          D.name = CTE.database_name
AND         RowNum = 1
WHERE       ( backup_start_date IS NULL OR backup_start_date < DATEADD(dd,-1,GetDate()) )
AND         recovery_model_desc != 'SIMPLE'
ORDER BY    D.name,type

USE master;  
GO  
SELECT query_plan,* 
from sys.dm_exec_requests  a
cross apply sys.dm_exec_query_plan (a.plan_handle)
where session_id<>@@SPID;  
GO


SELECT r.session_id AS [Session_Id]
    ,r.command AS [command]
    ,CONVERT(NUMERIC(6, 2), r.percent_complete) AS [% Complete]
    ,GETDATE() AS [Current Time]
    ,CONVERT(VARCHAR(20), DATEADD(ms, r.estimated_completion_time, GetDate()), 20) AS [Estimated Completion Time]
    ,CONVERT(NUMERIC(32, 2), r.total_elapsed_time / 1000.0 / 60.0) AS [Elapsed Min]
    ,CONVERT(NUMERIC(32, 2), r.estimated_completion_time / 1000.0 / 60.0) AS [Estimated Min]
    ,CONVERT(NUMERIC(32, 2), r.estimated_completion_time / 1000.0 / 60.0 / 60.0) AS [Estimated Hours]
    ,CONVERT(VARCHAR(1000), (
            SELECT SUBSTRING(TEXT, r.statement_start_offset / 2, CASE
                        WHEN r.statement_end_offset = - 1
                            THEN 1000
                        ELSE (r.statement_end_offset - r.statement_start_offset) / 2
                        END) 'Statement text'
            FROM sys.dm_exec_sql_text(sql_handle)
            ))
FROM sys.dm_exec_requests r
WHERE command like 'RESTORE%'
or  command like 'BACKUP%'


DECLARE @ObjectName AS VARCHAR(100)

SET @ObjectName =
'[RservRdl].[AdventureWorks2017].[HumanResources].[Employee]'

SELECT PARSENAME(@ObjectName, 4) AS ServerName,
PARSENAME(@ObjectName, 3) AS DatabaseName,
PARSENAME(@ObjectName, 2) AS SchemaName,
PARSENAME(@ObjectName, 1) AS TableName

