/* pass the below Job names to get the details logs for the job
DSS_Credit_Due_Vs_Col,DSS_COL,DSS_DDD
*/
DECLARE @Job_Name varchar(50)='DSS_DDD',@date date='2022-02-03',@SQL_QUERY NVARCHAR(max)

set @SQL_QUERY='

set nocount on;
set @text=isnull(@text,''DDD'')

if @text=''DSS_COL''

BEGIN

IF OBJECT_ID(''tempdb..##tmp_kar_job_status'') IS NOT NULL                                          
DROP TABLE ##tmp_kar_job_status
SELECT a.JOB_NAME,PROC_NAME,concat(STEP_ID,''-'',STEP_SUBID,''-'',a.STEPNAME)STEPNAME
,cast(b.CompleatedTime as smalldatetime)CompleatedTime
into ##tmp_kar_job_status
FROM (select ''OD''JOB_NAME,''udp_daily_disbursements_DP''PROC_NAME,''1''STEP_ID ,''1''STEP_SUBID ,''t_disbursements-Update-Branch_USR_State''STEPNAME
UNION ALL select ''OD'',''udp_daily_disbursements_DP'',''1'' ,''2'' ,''t_disbursements-Update-PM_AM_DM''
UNION ALL select ''OD'',''udp_daily_disbursements_DP'',''1'' ,''3'' ,''t_disbursements_summary_Start''
UNION ALL select ''OD'',''udp_daily_disbursements_DP'',''1'' ,''4'' ,''t_disbursements_summary_Finished''
UNION ALL select ''OD'',''udp_daily_disbursements_DP'',''1'' ,''5'' ,''Disb Data Summary Compleated''
UNION ALL select ''OD'',''udp_daily_OD_Anytime'',''2'' ,''1'' ,''Live_DDvsC-t_loan_Daily_ODs_BEOD_or_AEOD created''
UNION ALL select ''OD'',''udp_daily_OD_Anytime'',''2'' ,''2'' ,''Daily OD Compleated''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''1'' ,''Live_DDvsC-t_collections0 Creations Started''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''2'' ,''Live_DDvsC-t_collections0 table Created''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''3'' ,''Live_DDvsC-CalculationsUpdated''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''4'' ,''Live_DDvsC-DueDate for Apr17 updated''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''5'' ,''Live_DDvsC-##TempTables_TAC Created''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''6'' ,''Live_DDvsC-##TempTables_TGM Created''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''7'' ,''Live_DDvsC-##TempTables_TC Created''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''8'' ,''Live_DDvsC-##TempTables Created''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''9'' ,''Live_DDvsC-Update-Product_ClientID''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''10'' ,''Live_DDvsC-Update-ClientBranchID_ClientName''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''11'' ,''Live_DDvsC-Update-Center_Group''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''12'' ,''Live_DDvsC-Update-Phones''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''13'' ,''Live_DDvsC-Update-Branch_USR_State''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''14'' ,''Live_DDvsC-Update-Region_Branch''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''15'' ,''Live_DDvsC-Update-PM_AM_DM''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''16'' ,''Live_DDvsC-Update-PrductDetails''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''17'' ,''Live_DDvsC-Update-Rligin,Cast,CRS&LO''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''18'' ,''Live_DDvsC-t_rpt_collections_summary table Creation Started''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''19'' ,''Live_DDvsC-t_rpt_collections_summary table Created''
UNION ALL select ''OD'',''udp_InstDues_vs_Coll'',''3'',''20'' ,''InstDues Vs Coll Compleated''
UNION ALL select ''OD'',''SP_OD_BRNET'',''4'',''1'' ,''OD Table Strated''
UNION ALL select ''OD'',''SP_OD_BRNET'',''4'',''2'' ,''OD Table Compleated''
--UNION ALL select ''OD'',''r_UJJ_Daily_IOD_MIS'',''5'' ,''1'' ,''Prev_IOD_Run_Already ''
--UNION ALL select ''OD'',''r_UJJ_Daily_IOD_MIS'',''5'' ,''2'' ,''IOD_Prev_completed''
UNION ALL select ''OD'',''r_UJJ_Daily_IOD_MIS'',''5'' ,''1'' ,''Daily_IOD_Completed''
UNION ALL select ''OD'',''USP_RPT_Glow_Daily_Rejects'',''6'',''1'',''Glow_Rejects_Daily_Completed'')A
left JOIN (select * from [10.10.101.38].[MIS_DB].[DBO].SERIES WHERE CAST(CompleatedTime AS DATE)=ISNULL(@date,CAST(GETDATE() AS DATE)) )B ON A.StepName=B.STEPNAME
ORDER BY b.CompleatedTime desC

--select JOB_NAME,PROC_NAME,STEPNAME,StartTime,EndTime
--,STUFF(CONVERT(VARCHAR(20),CAST(EndTime AS SMALLDATETIME)-CAST(StartTime AS SMALLDATETIME),114),1,2
--,DATEDIFF(hh,0,CAST(EndTime AS SMALLDATETIME)-CAST(StartTime AS SMALLDATETIME))) Duration 
--from(select JOB_NAME,PROC_NAME,STEPNAME,lag(CompleatedTime, 1,CompleatedTime) 
--OVER(ORDER BY CompleatedTime ASC) AS StartTime,CompleatedTime EndTime 
--from ##tmp_kar_job_status 
--)a order by 3 asc


IF OBJECT_ID(''tempdb..##tmp_kar_job_status_F'') IS NOT NULL                                          
DROP TABLE ##tmp_kar_job_status_F
select JOB_NAME,PROC_NAME,STEPNAME,isnull(StartTime,EndTime)StartTime,EndTime
,STUFF(CONVERT(VARCHAR(20),CAST(EndTime AS SMALLDATETIME)-CAST(isnull(StartTime,EndTime) AS SMALLDATETIME),114),1,2
,DATEDIFF(hh,0,CAST(EndTime AS SMALLDATETIME)-CAST(isnull(StartTime,EndTime) AS SMALLDATETIME))) Duration 
INTO ##tmp_kar_job_status_F
from(select JOB_NAME,PROC_NAME,STEPNAME,lag(CompleatedTime, 1,CompleatedTime) 
OVER(ORDER BY CompleatedTime ASC) AS StartTime,CompleatedTime EndTime 
from ##tmp_kar_job_status
)a order by 3 asc


SELECT a.JOB_NAME,a.PROC_NAME,a.STEPNAME,isnull(a.StartTime,(select max(endtime) from ##tmp_kar_job_status1_F where PROC_NAME=B.PROC_NAME))StartTime
,a.EndTime,a.Duration
,b.sessionid,proc_starttime,loginname,status,program_name 
,query,clientnetaddress
FROM ##tmp_kar_job_status_F A
LEFT JOIN (
SELECT * FROM ##tmp_kar_job_status_F A
INNER JOIN (select DISTINCT 
req.session_id as sessionid,
req.start_time as proc_starttime,
ses.login_name as loginname,
req.status as status,
ses.program_name as program_name,
object_name(sqltext.objectid,sqltext.dbid) as procedurename,
substring(sqltext.text,(req.statement_start_offset/2)+1, 
((case req.statement_end_offset when -1 then datalength(sqltext.text) else req.statement_end_offset end - req.statement_start_offset)/2)+1) as query,
dec.client_net_address as clientnetaddress
from sys.dm_exec_requests req
cross apply sys.dm_exec_sql_text(sql_handle) as sqltext
inner join sys.dm_exec_sessions ses on req.session_id=ses.session_id
inner join sys.dm_exec_connections as dec on ses.session_id=dec.session_id) B ON B.procedurename =A.PROC_NAME
WHERE CONCAT(LEFT(STEPNAME,1),SUBSTRING(STEPNAME,3,1))=(
SELECT MIN(CONCAT(LEFT(STEPNAME,1),SUBSTRING(STEPNAME,3,1))) 
FROM ##tmp_kar_job_status_F
WHERE STARTTIME IS NULL))B ON A.STEPNAME=B.STEPNAME
ORDER BY 3 ASC

END
else IF @TEXT=''DSS_Credit_Due_Vs_Col''
BEGIN 

IF OBJECT_ID(''tempdb..##tmp_kar_job_status1'') IS NOT NULL                                          
DROP TABLE ##tmp_kar_job_status1
SELECT a.JOB_NAME,PROC_NAME,concat(STEP_ID,''-'',STEP_SUBID,''-'',a.STEPNAME)STEPNAME
,cast(b.CompleatedTime as smalldatetime)CompleatedTime
--,row_number() over(partition by concat(STEP_ID,''-'',STEP_SUBID,''-'',a.STEPNAME) order by CompleatedTime)RN
  into ##tmp_kar_job_status1
FROM (select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS''PROC_NAME,''1'' STEP_ID,''1'' STEP_SUBID,''Daily_MB_Table_Created'' STEPNAME
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS''PROC_NAME,''1'' STEP_ID,''2'' STEP_SUBID,''Daily_MB_Loan_Cycle_Updated''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS''PROC_NAME,''1'' STEP_ID,''3'' STEP_SUBID,''Daily_MB_Cashless_Table_Created''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS''PROC_NAME,''1'' STEP_ID,''4'' STEP_SUBID,''Daily_Credit_MB_Repayment_Summary_Started''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS''PROC_NAME,''1'' STEP_ID,''5'' STEP_SUBID,''Daily_Credit_MB_Repayment_Summary_Created''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS''PROC_NAME,''1'' STEP_ID,''6'' STEP_SUBID,''Daily_Credit_MB_Repayment_Completed''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS''PROC_NAME,''1'' STEP_ID,''7'' STEP_SUBID,''Credit_Repayment_Table_Copied''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS''PROC_NAME,''1'' STEP_ID,''8'' STEP_SUBID,''Credit_Repayment_Table_Creation_Completed''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS_Updates''PROC_NAME,''2'' STEP_ID,''1'' STEP_SUBID,''Credit_Repayment_Table_Updates_1_Start''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS_Updates''PROC_NAME,''2'' STEP_ID,''2'' STEP_SUBID,''Credit_Repayment_Table_Updates_1_End''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS_Updates''PROC_NAME,''2'' STEP_ID,''3'' STEP_SUBID,''Credit_Repayment_Table_Updates_2_Start''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS_Updates''PROC_NAME,''2'' STEP_ID,''4'' STEP_SUBID,''Credit_Repayment_Table_Updates_2_End''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS_Updates''PROC_NAME,''2'' STEP_ID,''5'' STEP_SUBID,''Credit_Repayment_Table_Updates_3_Start''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS_Updates''PROC_NAME,''2'' STEP_ID,''6'' STEP_SUBID,''Credit_Repayment_Table_Updates_3_End''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS_Updates''PROC_NAME,''2'' STEP_ID,''7'' STEP_SUBID,''Credit_Repayment_Table_Updates_4_Start''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS_Updates''PROC_NAME,''2'' STEP_ID,''8'' STEP_SUBID,''Credit_Repayment_Table_Updates_4_End''
UNION ALL select ''CREDIT_DUES_VS_COLLECTION''JOB_NAME,''r_UJJ_Credit_MB_Repayment_Data_MIS_Updates''PROC_NAME,''2'' STEP_ID,''9'' STEP_SUBID,''Hourly_Credit_Repayment_Table_Created''
 )A
left JOIN (select * from [10.10.101.38].[MIS_DB].[DBO].SERIES WHERE CAST(CompleatedTime AS DATE)=ISNULL(@date,CAST(GETDATE() AS DATE)))B ON A.StepName=B.STEPNAME
ORDER BY b.CompleatedTime desc

IF OBJECT_ID(''tempdb..##tmp_kar_job_status1_F'') IS NOT NULL                                          
DROP TABLE ##tmp_kar_job_status1_F
select JOB_NAME,PROC_NAME,STEPNAME,isnull(StartTime,EndTime)StartTime,EndTime
,STUFF(CONVERT(VARCHAR(20),CAST(EndTime AS SMALLDATETIME)-CAST(isnull(StartTime,EndTime) AS SMALLDATETIME),114),1,2
,DATEDIFF(hh,0,CAST(EndTime AS SMALLDATETIME)-CAST(isnull(StartTime,EndTime) AS SMALLDATETIME))) Duration 
INTO ##tmp_kar_job_status1_F
from(select JOB_NAME,PROC_NAME,STEPNAME,lag(CompleatedTime, 1,CompleatedTime) 
OVER(ORDER BY CompleatedTime ASC) AS StartTime,CompleatedTime EndTime 
from ##tmp_kar_job_status1
)a order by 3 asc

SELECT a.JOB_NAME,a.PROC_NAME,a.STEPNAME,isnull(a.StartTime,(select max(endtime) from ##tmp_kar_job_status1_F where PROC_NAME=B.PROC_NAME))StartTime
,a.EndTime,a.Duration
,b.sessionid,proc_starttime,loginname,status,program_name 
,query,clientnetaddress
FROM ##tmp_kar_job_status1_F A
LEFT JOIN (
SELECT * FROM ##tmp_kar_job_status1_F A
INNER JOIN (select DISTINCT 
req.session_id as sessionid,
req.start_time as proc_starttime,
ses.login_name as loginname,
req.status as status,
ses.program_name as program_name,
object_name(sqltext.objectid,sqltext.dbid) as procedurename,
substring(sqltext.text,(req.statement_start_offset/2)+1, 
((case req.statement_end_offset when -1 then datalength(sqltext.text) else req.statement_end_offset end - req.statement_start_offset)/2)+1) as query,
dec.client_net_address as clientnetaddress
from sys.dm_exec_requests req
cross apply sys.dm_exec_sql_text(sql_handle) as sqltext
inner join sys.dm_exec_sessions ses on req.session_id=ses.session_id
inner join sys.dm_exec_connections as dec on ses.session_id=dec.session_id) B ON B.procedurename =A.PROC_NAME
WHERE CONCAT(LEFT(STEPNAME,1),SUBSTRING(STEPNAME,3,1))=(
SELECT MIN(CONCAT(LEFT(STEPNAME,1),SUBSTRING(STEPNAME,3,1))) 
FROM ##tmp_kar_job_status1_F
WHERE STARTTIME IS NULL))B ON A.STEPNAME=B.STEPNAME
ORDER BY 3 ASC


END

else IF (@TEXT=''DSS_DDD'')
BEGIN

IF OBJECT_ID(''tempdb..##tmp_kar_job_status2'') IS NOT NULL                                          
DROP TABLE ##tmp_kar_job_status2
SELECT a.JOB_NAME,PROC_NAME,concat(STEP_ID,''-'',STEP_SUBID,''-'',a.STEPNAME)STEPNAME
,cast(b.CompleatedTime as smalldatetime)CompleatedTime
--,row_number() over(partition by concat(STEP_ID,''-'',STEP_SUBID,''-'',a.STEPNAME) order by CompleatedTime)RN
  into ##tmp_kar_job_status2
FROM (select ''DDD''JOB_NAME,''DDD_STEP_1''PROC_NAME,''1'' STEP_ID,''1'' STEP_SUBID,''DDD_TRUNCATE_COMPLETED'' STEPNAME
UNION ALL select ''DDD'',''DDD_STEP_1'',''1'',''2'',''T_rpt_daily_portfolio_Status''
UNION ALL select ''DDD'',''DDD_STEP_1'',''1'',''3'',''DDD_STEP1_COMPLETED''
UNION ALL select ''DDD'',''DDD_STEP_2'',''2'',''1'',''DDD_STEP_2_UPDATE_COMPLETED'' )A
left JOIN (select * from [10.10.101.38].[MIS_DB].[DBO].SERIES 
WHERE CAST(CompleatedTime AS DATE)=ISNULL(@date,CAST(GETDATE() AS DATE)))B ON A.StepName=B.STEPNAME
ORDER BY b.CompleatedTime desc

select JOB_NAME,PROC_NAME,STEPNAME,StartTime,EndTime
,STUFF(CONVERT(VARCHAR(20),CAST(EndTime AS SMALLDATETIME)-CAST(StartTime AS SMALLDATETIME),114),1,2
,DATEDIFF(hh,0,CAST(EndTime AS SMALLDATETIME)-CAST(StartTime AS SMALLDATETIME))) Duration 
from(select JOB_NAME,PROC_NAME,STEPNAME,lag(CompleatedTime, 1,CompleatedTime) 
OVER(ORDER BY CompleatedTime ASC) AS StartTime,CompleatedTime EndTime 
from ##tmp_kar_job_status2 
where DATENAME(HOUR, CompleatedTime)<12)a order by 3 asc

select JOB_NAME,PROC_NAME,STEPNAME,StartTime,EndTime
,STUFF(CONVERT(VARCHAR(20),CAST(EndTime AS SMALLDATETIME)-CAST(StartTime AS SMALLDATETIME),114),1,2
,DATEDIFF(hh,0,CAST(EndTime AS SMALLDATETIME)-CAST(StartTime AS SMALLDATETIME))) Duration 
from(select JOB_NAME,PROC_NAME,STEPNAME,lag(CompleatedTime, 1,CompleatedTime) 
OVER(ORDER BY CompleatedTime ASC) AS StartTime,CompleatedTime EndTime from ##tmp_kar_job_status2 
where DATENAME(HOUR, CompleatedTime)>12)a order by 3 asc

END

ELSE 
BEGIN
SELECT @text Job_Name,''PROVIDED JOBNAME IS NOT IN THE AUTOMATED LIST''
END' 
EXECUTE sp_executesql @SQL_QUERY,N'@TEXT NVARCHAR(50),@date date', @Job_Name,@date

