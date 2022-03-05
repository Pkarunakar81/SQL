

--to enable the cdc
USE [CDC Testing]
GO
EXEC sys.sp_cdc_enable_db

--ENABLE THE CDC FOR TABLE

USE [CDC Testing]
GO
EXEC sys.sp_cdc_enable_table
@source_schema = N'Production', -- Schema Name. Example, HR, Sales etc
@source_name = N'Product', -- Table Name
@role_name = NULL -- Any specified roles
GO
--sys.sp_MScdc_capture_job
--sys.sp_MScdc_cleanup_job

UPDATE A SET A.NAME='Blade1' from [Production].[Product] A
WHERE ProductID=316

EXEC sys.sp_cdc_help_change_data_capture 
GO

--- TO DISABLE THE CDC FOR TABLE

USE [CDC Testing]
GO
EXEC sys.sp_cdc_disable_table
@source_schema = N'Production', -- Schema Name. Example, HR, Sales etc
@source_name = N'Product', -- Table Name
@capture_instance = Production_Product
GO

--Disable SQL Server CDC on Database

USE [CDC Testing]
GO
EXEC sys.sp_cdc_disable_db

--Enable SQL Server CDC on multiple Columns

USE [CDC Testing]
GO
EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name = N'Employee',
@role_name = NULL,
@captured_column_list = '[EmpID], [FirstName]'
GO